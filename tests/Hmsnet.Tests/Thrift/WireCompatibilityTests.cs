using Hmsnet.Api.Thrift;
using Hmsnet.Tests.Helpers;

namespace Hmsnet.Tests.Thrift;

/// <summary>
/// Regression guard against the class of bug where our hand-rolled Thrift wire format drifts from
/// the official <c>hive_metastore.thrift</c> schema (wrong field ids / types). The test client is
/// pinned to the official field ids, so:
///   • the round-trip test proves the server reads+writes every Table/SD field a real HiveServer2 sends;
///   • the field-id test asserts the exact on-the-wire layout (id → type) matches the Hive schema,
///     so any drift — in either direction — fails the build instead of only breaking against real Hive.
/// </summary>
[TestClass]
public class WireCompatibilityTests
{
    private static ThriftTestServer _server = null!;
    private ThriftTestClient _client = null!;

    [ClassInitialize]
    public static async Task ClassInitializeAsync(TestContext _) => _server = await ThriftTestServer.StartAsync();

    [ClassCleanup]
    public static async Task ClassCleanupAsync() => await _server.DisposeAsync();

    [TestInitialize]
    public async Task TestInitializeAsync() => _client = await ThriftTestClient.ConnectAsync(_server.Port);

    [TestCleanup]
    public async Task TestCleanupAsync() => await _client.DisposeAsync();

    private static ThriftTable RichTable(string db, string name) => new()
    {
        TableName = name,
        DbName = db,
        Owner = "svc",
        TableType = "EXTERNAL_TABLE",
        CreateTime = 1_700_000_000,
        Columns =
        [
            new ThriftFieldSchema("id", "bigint", "primary id"),
            new ThriftFieldSchema("amount", "decimal(10,2)", null),
            new ThriftFieldSchema("note", "string", null),
        ],
        Sd = new ThriftStorageDescriptor(
            Location: "abfss://wh/orders",
            InputFormat: "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            OutputFormat: "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            Compressed: true,
            NumBuckets: 4,
            SerDeInfo: new ThriftSerDeInfo("orders", "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                new Dictionary<string, string> { ["field.delim"] = "," }),
            BucketCols: null,
            SortCols: null,
            Parameters: new Dictionary<string, string> { ["orc.compress"] = "ZLIB" }),
        PartitionKeys = [new ThriftFieldSchema("dt", "string", "partition date")],
        Parameters = new Dictionary<string, string> { ["EXTERNAL"] = "true", ["comment"] = "orders table" },
    };

    [TestMethod]
    public async Task Table_round_trips_every_field_a_real_hs2_sends()
    {
        await _client.CreateDatabaseAsync("wire_db");
        await _client.CreateTableAsync(RichTable("wire_db", "orders"));
        var got = await _client.GetTableAsync("wire_db", "orders");

        Assert.AreEqual("orders", got.TableName);
        Assert.AreEqual("wire_db", got.DbName);
        Assert.AreEqual("EXTERNAL_TABLE", got.TableType);
        Assert.IsNotNull(got.Sd);
        // These are exactly the fields the old (wrong) field ids scrambled:
        Assert.AreEqual("abfss://wh/orders", got.Sd!.Location);
        Assert.AreEqual("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", got.Sd.InputFormat);
        Assert.AreEqual("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat", got.Sd.OutputFormat);
        Assert.IsTrue(got.Sd.Compressed);
        Assert.AreEqual(4, got.Sd.NumBuckets);
        Assert.AreEqual("org.apache.hadoop.hive.ql.io.orc.OrcSerde", got.Sd.SerDeInfo.SerializationLib);
        // columns survive with their types
        Assert.IsNotNull(got.Sd.Cols);
        Assert.IsTrue(got.Sd.Cols!.Any(c => c.Name == "id" && c.Type == "bigint"));
        Assert.IsTrue(got.Sd.Cols.Any(c => c.Name == "amount" && c.Type == "decimal(10,2)"));
        // partition keys survive
        Assert.IsNotNull(got.PartitionKeys);
        Assert.IsTrue(got.PartitionKeys!.Any(p => p.Name == "dt" && p.Type == "string"));
        // table parameters survive
        Assert.IsNotNull(got.Parameters);
        Assert.AreEqual("true", got.Parameters!.GetValueOrDefault("EXTERNAL"));
    }

    [TestMethod]
    public async Task Table_created_with_a_catalog_qualified_db_is_listed_under_the_plain_db()
    {
        // Hive 4.x encodes the catalog into the db-name string ("@hive#db") for the legacy APIs, so a
        // real HS2 can create a table whose DbName arrives catalog-qualified. The read path strips the
        // catalog; the create path must strip it too, or the table lands under an unresolvable db and
        // SHOW TABLES comes back empty — exactly the "northwind has 0 tables" production symptom.
        await _client.CreateDatabaseAsync("catalog_db");
        await _client.CreateTableAsync(RichTable("@hive#catalog_db", "orders"));

        var byPlain = await _client.GetTableAsync("catalog_db", "orders");
        Assert.AreEqual("orders", byPlain.TableName);

        var listed = await _client.GetAllTablesAsync("catalog_db");
        CollectionAssert.Contains(listed.ToList(), "orders",
            "table created with a catalog-qualified db must be listed under the plain db name");
    }

    [TestMethod]
    public async Task Get_database_req_unwraps_the_request_and_finds_an_existing_db()
    {
        // Real Hive 4 HS2 validates the target DB during CREATE TABLE via get_database_req, whose one
        // argument is a GetDatabaseRequest nested under the method args wrapper. If the server reads the
        // name at the wrapper level it comes back empty and HS2 reports a freshly created database as
        // "does not exist" — which blocked every sample-data CREATE TABLE.
        await _client.CreateDatabaseAsync("req_db", owner: "svc");
        var db = await _client.GetDatabaseReqAsync("req_db");
        Assert.AreEqual("req_db", db.Name);
    }

    [TestMethod]
    public async Task Get_table_objects_by_name_req_with_no_names_lists_all_tables()
    {
        // SHOW TABLES on Hive 4 HS2 is get_table_objects_by_name_req with the db name and an EMPTY name
        // list, expecting every table back. The old handler read the request at the wrapper level, got
        // an empty db, and returned nothing — the "No tables" symptom even though the tables exist.
        await _client.CreateDatabaseAsync("show_db");
        await _client.CreateTableAsync(RichTable("show_db", "orders"));
        await _client.CreateTableAsync(RichTable("show_db", "customers"));

        var listed = await _client.GetTableObjectsByNameReqAsync("show_db", []);
        CollectionAssert.AreEquivalent(new List<string> { "orders", "customers" }, listed);
    }

    [TestMethod]
    public async Task Table_wire_layout_matches_the_official_hive_field_ids()
    {
        var bytes = await ThriftTestClient.SerializeTableAsync(RichTable("db", "t"));
        var proto = new ThriftBinaryProtocol(new MemoryStream(bytes));

        var table = new Dictionary<short, TType>();
        Dictionary<short, TType>? sd = null;
        await proto.ReadStructBeginAsync(default);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(default);
            if (f.Type == TType.Stop) break;
            table[f.Id] = f.Type;
            if (f.Id == 7 && f.Type == TType.Struct) sd = await ReadFieldMapAsync(proto);
            else await proto.SkipAsync(f.Type, default);
            await proto.ReadFieldEndAsync(default);
        }
        await proto.ReadStructEndAsync(default);

        // Official Hive Table ids (hive_metastore.thrift):
        Assert.AreEqual(TType.String, table[1], "tableName@1");
        Assert.AreEqual(TType.String, table[2], "dbName@2");
        Assert.AreEqual(TType.Struct, table[7], "sd@7");
        Assert.AreEqual(TType.List, table[8], "partitionKeys@8");
        Assert.AreEqual(TType.Map, table[9], "parameters@9");
        Assert.AreEqual(TType.String, table[12], "tableType@12");
        Assert.IsFalse(table.ContainsKey(15), "nothing at the old wrong tableType id 15");

        // Official Hive StorageDescriptor ids:
        Assert.IsNotNull(sd);
        Assert.AreEqual(TType.List, sd![1], "sd.cols@1");
        Assert.AreEqual(TType.String, sd[2], "sd.location@2");
        Assert.AreEqual(TType.String, sd[3], "sd.inputFormat@3");
        Assert.AreEqual(TType.String, sd[4], "sd.outputFormat@4");
        Assert.AreEqual(TType.Bool, sd[5], "sd.compressed@5");
        Assert.AreEqual(TType.I32, sd[6], "sd.numBuckets@6");
        Assert.AreEqual(TType.Struct, sd[7], "sd.serdeInfo@7");
        Assert.AreEqual(TType.Map, sd[10], "sd.parameters@10");
        Assert.IsFalse(sd.ContainsKey(12), "nothing at the old wrong sd.parameters id 12");
    }

    private static async Task<Dictionary<short, TType>> ReadFieldMapAsync(ThriftBinaryProtocol proto)
    {
        var map = new Dictionary<short, TType>();
        await proto.ReadStructBeginAsync(default);
        while (true)
        {
            var f = await proto.ReadFieldBeginAsync(default);
            if (f.Type == TType.Stop) break;
            map[f.Id] = f.Type;
            await proto.SkipAsync(f.Type, default);
            await proto.ReadFieldEndAsync(default);
        }
        await proto.ReadStructEndAsync(default);
        return map;
    }
}
