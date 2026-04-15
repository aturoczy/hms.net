using Hmsnet.Api.Thrift;
using Hmsnet.Tests.Helpers;

namespace Hmsnet.Tests.Thrift;

[TestClass]
[DoNotParallelize]
public class ThriftInteropTests
{
    private static ThriftTestServer _server = null!;
    private ThriftTestClient _client = null!;

    [ClassInitialize]
    public static async Task ClassInitializeAsync(TestContext _)
    {
        _server = await ThriftTestServer.StartAsync();
    }

    [ClassCleanup]
    public static async Task ClassCleanupAsync()
    {
        await _server.DisposeAsync();
    }

    [TestInitialize]
    public async Task TestInitializeAsync()
    {
        _client = await ThriftTestClient.ConnectAsync(_server.Port);
    }

    [TestCleanup]
    public async Task TestCleanupAsync()
    {
        await _client.DisposeAsync();
    }

    // ── Database behaviour ────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetAllDatabases_InitiallyEmpty()
    {
        var dbs = await _client.GetAllDatabasesAsync();
        Assert.AreEqual(0, dbs.Count);
    }

    [TestMethod]
    public async Task CreateDatabase_NormalizesNameToLowercase()
    {
        await _client.CreateDatabaseAsync("MyDB_01");
        var dbs = await _client.GetAllDatabasesAsync();
        CollectionAssert.Contains(dbs, "mydb_01");
        CollectionAssert.DoesNotContain(dbs, "MyDB_01");
    }

    [TestMethod]
    public async Task CreateDatabase_SetsDefaultWarehouseLocation()
    {
        await _client.CreateDatabaseAsync("warehousedb_02");
        var db = await _client.GetDatabaseAsync("warehousedb_02");
        StringAssert.Contains(db.LocationUri, "warehousedb_02.db");
    }

    [TestMethod]
    public async Task CreateDatabase_ThenGetDatabase_RoundTrip()
    {
        await _client.CreateDatabaseAsync("roundtripdb_03", description: "my desc",
            location: "hdfs:///custom/path", owner: "testowner");
        var db = await _client.GetDatabaseAsync("roundtripdb_03");
        Assert.AreEqual("roundtripdb_03", db.Name);
        Assert.AreEqual("my desc", db.Description);
        Assert.AreEqual("hdfs:///custom/path", db.LocationUri);
        Assert.AreEqual("testowner", db.OwnerName);
    }

    [TestMethod]
    public async Task GetDatabase_CaseInsensitive()
    {
        await _client.CreateDatabaseAsync("casedb_04");
        var db = await _client.GetDatabaseAsync("CASEDB_04");
        Assert.AreEqual("casedb_04", db.Name);
    }

    [TestMethod]
    public async Task CreateDatabase_Duplicate_ThrowsException()
    {
        await _client.CreateDatabaseAsync("dupdb_05");
        await AssertEx.ThrowsAsync<ThriftApplicationException>(
            () => _client.CreateDatabaseAsync("dupdb_05"));
    }

    [TestMethod]
    public async Task GetDatabase_NotFound_ThrowsException()
    {
        await AssertEx.ThrowsAsync<ThriftApplicationException>(
            () => _client.GetDatabaseAsync("nonexistent_06"));
    }

    [TestMethod]
    public async Task DropDatabase_RemovesDatabase()
    {
        await _client.CreateDatabaseAsync("dropdb_07");
        var before = await _client.GetAllDatabasesAsync();
        CollectionAssert.Contains(before, "dropdb_07");

        await _client.DropDatabaseAsync("dropdb_07", cascade: true);
        var after = await _client.GetAllDatabasesAsync();
        CollectionAssert.DoesNotContain(after, "dropdb_07");
    }

    // ── Table behaviour ───────────────────────────────────────────────────────

    [TestMethod]
    public async Task CreateTable_ThenGetTable_RoundTrip()
    {
        await _client.CreateDatabaseAsync("tabledb_08");
        var tbl = MakeTestTable("tabledb_08", "events_08");
        await _client.CreateTableAsync(tbl);
        var got = await _client.GetTableAsync("tabledb_08", "events_08");
        Assert.AreEqual("events_08", got.TableName);
        Assert.AreEqual("tabledb_08", got.DbName);
        Assert.AreEqual("testowner", got.Owner);
        Assert.AreEqual("MANAGED_TABLE", got.TableType);
        Assert.IsNotNull(got.Sd);
        Assert.AreEqual("org.apache.orc.mapreduce.OrcInputFormat", got.Sd.InputFormat);
        Assert.AreEqual("/warehouse/tabledb_08/events_08", got.Sd.Location);
    }

    [TestMethod]
    public async Task CreateTable_PartitionKeys_StoredSeparately()
    {
        await _client.CreateDatabaseAsync("partkeysdb_09");
        var tbl = MakeTestTable("partkeysdb_09", "events_09");
        await _client.CreateTableAsync(tbl);
        var got = await _client.GetTableAsync("partkeysdb_09", "events_09");
        Assert.IsNotNull(got.PartitionKeys);
        Assert.IsTrue(got.PartitionKeys.Any(pk => pk.Name == "dt"),
            "Expected partition key 'dt' in PartitionKeys");
        var fields = await _client.GetFieldsAsync("partkeysdb_09", "events_09");
        Assert.IsFalse(fields.Any(f => f.Name == "dt"),
            "Partition key 'dt' should not appear in GetFields result");
    }

    [TestMethod]
    public async Task GetAllTables_ReturnsTableNames()
    {
        await _client.CreateDatabaseAsync("alltablesdb_10");
        await _client.CreateTableAsync(MakeTestTable("alltablesdb_10", "events_10"));
        var tables = await _client.GetAllTablesAsync("alltablesdb_10");
        CollectionAssert.Contains(tables, "events_10");
    }

    [TestMethod]
    public async Task GetFields_ReturnsOnlyDataColumns()
    {
        await _client.CreateDatabaseAsync("fieldsdb_11");
        await _client.CreateTableAsync(MakeTestTable("fieldsdb_11", "events_11"));
        var fields = await _client.GetFieldsAsync("fieldsdb_11", "events_11");
        Assert.IsTrue(fields.Any(f => f.Name == "id"), "Expected 'id' column");
        Assert.IsTrue(fields.Any(f => f.Name == "name"), "Expected 'name' column");
        Assert.IsFalse(fields.Any(f => f.Name == "dt"), "Should not have partition key 'dt'");
    }

    [TestMethod]
    public async Task GetSchema_ReturnsAllColumnsDataFirst()
    {
        await _client.CreateDatabaseAsync("schemadb_12");
        await _client.CreateTableAsync(MakeTestTable("schemadb_12", "events_12"));
        var schema = await _client.GetSchemaAsync("schemadb_12", "events_12");
        Assert.IsTrue(schema.Count >= 3, "Expected at least 3 columns (id, name, dt)");
        var names = schema.Select(f => f.Name).ToList();
        int idIdx = names.IndexOf("id");
        int dtIdx = names.IndexOf("dt");
        Assert.IsTrue(idIdx >= 0, "Expected 'id' in schema");
        Assert.IsTrue(dtIdx >= 0, "Expected 'dt' in schema");
        Assert.IsTrue(idIdx < dtIdx, "Data column 'id' should appear before partition key 'dt'");
    }

    // ── Partition behaviour ───────────────────────────────────────────────────

    [TestMethod]
    public async Task AddPartition_ThenGetPartition_RoundTrip()
    {
        await _client.CreateDatabaseAsync("addpartdb_13");
        await _client.CreateTableAsync(MakeTestTable("addpartdb_13", "events_13"));
        var part = new ThriftPartition(
            ["2024-01-01"], "addpartdb_13", "events_13", 0, 0,
            new ThriftStorageDescriptor(
                "/warehouse/addpartdb_13/events_13/dt=2024-01-01",
                "org.apache.orc.mapreduce.OrcInputFormat",
                "org.apache.orc.mapreduce.OrcOutputFormat",
                false, -1,
                new ThriftSerDeInfo(null, "org.apache.hadoop.hive.ql.io.orc.OrcSerde", null),
                null, null, null),
            null);
        var added = await _client.AddPartitionAsync(part);
        Assert.AreEqual("2024-01-01", added.Values?[0]);
        Assert.IsTrue(added.CreateTime > 0, "Expected createTime > 0");

        var fetched = await _client.GetPartitionAsync("addpartdb_13", "events_13", ["2024-01-01"]);
        Assert.IsNotNull(fetched.Values);
        Assert.AreEqual("2024-01-01", fetched.Values[0]);
        Assert.IsNotNull(fetched.Sd);
        StringAssert.Contains(fetched.Sd.Location, "dt=2024-01-01");
    }

    [TestMethod]
    public async Task GetPartitionNames_UsesEqualsSyntax()
    {
        await _client.CreateDatabaseAsync("partnamesdb_14");
        await _client.CreateTableAsync(MakeTestTable("partnamesdb_14", "events_14"));
        await _client.AddPartitionAsync(MakeTestPartition("partnamesdb_14", "events_14", "2024-02-01"));
        var names = await _client.GetPartitionNamesAsync("partnamesdb_14", "events_14");
        Assert.IsTrue(names.Count > 0, "Expected at least one partition name");
        Assert.IsTrue(names.Any(n => n.Contains('=')), "Partition name should use 'col=val' format");
    }

    [TestMethod]
    public async Task GetPartitions_RespectsMaxParts()
    {
        await _client.CreateDatabaseAsync("maxpartsdb_15");
        await _client.CreateTableAsync(MakeTestTable("maxpartsdb_15", "events_15"));
        await _client.AddPartitionAsync(MakeTestPartition("maxpartsdb_15", "events_15", "2024-01-01"));
        await _client.AddPartitionAsync(MakeTestPartition("maxpartsdb_15", "events_15", "2024-01-02"));
        await _client.AddPartitionAsync(MakeTestPartition("maxpartsdb_15", "events_15", "2024-01-03"));

        var limited = await _client.GetPartitionsAsync("maxpartsdb_15", "events_15", maxParts: 1);
        Assert.IsTrue(limited.Count <= 1, "Expected at most 1 partition when maxParts=1");
    }

    [TestMethod]
    public async Task DropPartition_ReturnsTrueOnSuccess()
    {
        await _client.CreateDatabaseAsync("droppartdb_16");
        await _client.CreateTableAsync(MakeTestTable("droppartdb_16", "events_16"));
        await _client.AddPartitionAsync(MakeTestPartition("droppartdb_16", "events_16", "2024-03-01"));

        var result = await _client.DropPartitionAsync("droppartdb_16", "events_16", ["2024-03-01"]);
        Assert.IsTrue(result, "Expected DropPartition to return true on success");
    }

    [TestMethod]
    public async Task GetPartition_NotFound_ThrowsException()
    {
        await _client.CreateDatabaseAsync("nopartdb_17");
        await _client.CreateTableAsync(MakeTestTable("nopartdb_17", "events_17"));
        await AssertEx.ThrowsAsync<ThriftApplicationException>(
            () => _client.GetPartitionAsync("nopartdb_17", "events_17", ["nonexistent"]));
    }

    // ── Compatibility stub behaviour ──────────────────────────────────────────

    [TestMethod]
    public async Task GetAllFunctions_ReturnsWithoutError()
    {
        var result = await _client.GetAllFunctionsAsync();
        Assert.IsTrue(result, "Expected GetAllFunctions to return true (no exception)");
    }

    [TestMethod]
    public async Task SetUgi_ReturnsEmptyList()
    {
        var groups = await _client.SetUgiAsync("hive", ["hive"]);
        Assert.IsNotNull(groups);
        Assert.AreEqual(0, groups.Count);
    }

    // ── Test helpers ──────────────────────────────────────────────────────────

    private static ThriftTable MakeTestTable(string dbName, string tableName) => new ThriftTable(
        tableName: tableName,
        dbName: dbName,
        owner: "testowner",
        tableType: "MANAGED_TABLE",
        createTime: 0,
        lastAccessTime: 0,
        retention: 0,
        sd: new ThriftStorageDescriptor(
            $"/warehouse/{dbName}/{tableName}",
            "org.apache.orc.mapreduce.OrcInputFormat",
            "org.apache.orc.mapreduce.OrcOutputFormat",
            false, -1,
            new ThriftSerDeInfo(null, "org.apache.hadoop.hive.ql.io.orc.OrcSerde", null),
            null, null, null),
        columns: [
            new ThriftFieldSchema("id", "bigint", null),
            new ThriftFieldSchema("name", "string", null)
        ],
        partitionKeys: [new ThriftFieldSchema("dt", "string", null)],
        viewOriginalText: null,
        viewExpandedText: null,
        parameters: null);

    private static ThriftPartition MakeTestPartition(string dbName, string tableName, string dtValue) => new ThriftPartition(
        [dtValue], dbName, tableName, 0, 0,
        new ThriftStorageDescriptor(
            $"/warehouse/{dbName}/{tableName}/dt={dtValue}",
            "org.apache.orc.mapreduce.OrcInputFormat",
            "org.apache.orc.mapreduce.OrcOutputFormat",
            false, -1,
            new ThriftSerDeInfo(null, "org.apache.hadoop.hive.ql.io.orc.OrcSerde", null),
            null, null, null),
        null);
}
