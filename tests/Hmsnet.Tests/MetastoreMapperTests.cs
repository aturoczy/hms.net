using Hmsnet.Core.DTOs;
using Hmsnet.Core.Mapping;
using Hmsnet.Core.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests;

[TestClass]
public class MetastoreMapperTests
{
    // ── HiveDatabase ──────────────────────────────────────────────────────────

    [TestMethod]
    public void DatabaseRequest_ToModel_MapsAllFields()
    {
        var dto = new DatabaseRequest(
            Name: "sales",
            Description: "Sales database",
            LocationUri: "hdfs:///user/hive/sales",
            OwnerName: "alice",
            OwnerType: "User",
            Parameters: new Dictionary<string, string> { ["env"] = "prod" });

        var model = dto.ToModel();

        Assert.AreEqual("sales", model.Name);
        Assert.AreEqual("Sales database", model.Description);
        Assert.AreEqual("hdfs:///user/hive/sales", model.LocationUri);
        Assert.AreEqual("alice", model.OwnerName);
        Assert.AreEqual(PrincipalType.User, model.OwnerType);
        Assert.AreEqual("prod", model.Parameters["env"]);
    }

    [TestMethod]
    public void HiveDatabase_ToDto_MapsAllFields()
    {
        var model = new HiveDatabase
        {
            Name = "orders",
            Description = "Order data",
            LocationUri = "hdfs:///orders",
            OwnerName = "bob",
            OwnerType = PrincipalType.Role,
            Parameters = new Dictionary<string, string> { ["k"] = "v" },
            CreateTime = 1_700_000_000L
        };

        var dto = model.ToDto();

        Assert.AreEqual("orders", dto.Name);
        Assert.AreEqual("Order data", dto.Description);
        Assert.AreEqual("hdfs:///orders", dto.LocationUri);
        Assert.AreEqual("bob", dto.OwnerName);
        Assert.AreEqual("Role", dto.OwnerType);
        Assert.AreEqual("v", dto.Parameters["k"]);
        Assert.AreEqual(1_700_000_000L, dto.CreateTime);
    }

    [TestMethod]
    public void Database_RoundTrip_PreservesAllValues()
    {
        var original = new DatabaseRequest(
            "mydb", "desc", "hdfs:///mydb", "owner", "Group",
            new Dictionary<string, string> { ["a"] = "1" });

        var dto = original.ToModel().ToDto();

        Assert.AreEqual("mydb", dto.Name);
        Assert.AreEqual("desc", dto.Description);
        Assert.AreEqual("Group", dto.OwnerType);
        Assert.AreEqual("1", dto.Parameters["a"]);
    }

    // ── HiveColumn ────────────────────────────────────────────────────────────

    [TestMethod]
    public void Column_ToDto_MapsAllFields()
    {
        var col = new HiveColumn
        {
            Name = "created_at",
            TypeName = "timestamp",
            Comment = "Row creation time",
            OrdinalPosition = 3,
            IsPartitionKey = false
        };

        var dto = col.ToDto();

        Assert.AreEqual("created_at", dto.Name);
        Assert.AreEqual("timestamp", dto.TypeName);
        Assert.AreEqual("Row creation time", dto.Comment);
        Assert.AreEqual(3, dto.OrdinalPosition);
        Assert.IsFalse(dto.IsPartitionKey);
    }

    [TestMethod]
    public void ColumnDto_ToModel_SetsIsPartitionKey()
    {
        var dto = new ColumnDto("dt", "string", null, 0, false);

        var dataCol = dto.ToModel(isPartitionKey: false);
        var partCol = dto.ToModel(isPartitionKey: true);

        Assert.IsFalse(dataCol.IsPartitionKey);
        Assert.IsTrue(partCol.IsPartitionKey);
    }

    // ── SerDeInfo ─────────────────────────────────────────────────────────────

    [TestMethod]
    public void SerDeInfo_RoundTrip_PreservesAllFields()
    {
        var model = new SerDeInfo
        {
            Name = "LazySimple",
            SerializationLib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            Parameters = new Dictionary<string, string> { ["field.delim"] = "," }
        };

        var dto = model.ToDto();
        var roundTripped = dto.ToModel();

        Assert.AreEqual("LazySimple", roundTripped.Name);
        Assert.AreEqual("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", roundTripped.SerializationLib);
        Assert.AreEqual(",", roundTripped.Parameters["field.delim"]);
    }

    // ── StorageDescriptor ─────────────────────────────────────────────────────

    [TestMethod]
    public void StorageDescriptor_ToDto_MapsLocation()
    {
        var sd = new StorageDescriptor
        {
            Location = "s3://bucket/path",
            InputFormat = "org.apache.hadoop.mapred.TextInputFormat",
            OutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            Compressed = false,
            NumBuckets = 4,
            BucketColumns = ["user_id"],
            SortColumns = [new SortOrder { Column = "ts", Order = SortDirection.Descending }],
            Parameters = new Dictionary<string, string>(),
            SkewedInfo = new SkewedInfo(),
            SerDeInfo = new SerDeInfo
            {
                SerializationLib = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                Parameters = new Dictionary<string, string>()
            }
        };

        var dto = sd.ToDto();

        Assert.AreEqual("s3://bucket/path", dto.Location);
        Assert.AreEqual(4, dto.NumBuckets);
        Assert.AreEqual(1, dto.BucketColumns!.Count);
        Assert.AreEqual("user_id", dto.BucketColumns![0]);
        Assert.AreEqual(1, dto.SortColumns!.Count);
        Assert.AreEqual("ts", dto.SortColumns![0].Column);
        Assert.AreEqual((int)SortDirection.Descending, dto.SortColumns![0].Order);
    }

    [TestMethod]
    public void StorageDescriptor_RoundTrip_PreservesFields()
    {
        var original = new StorageDescriptor
        {
            Location = "hdfs:///data/table",
            InputFormat = "TextInputFormat",
            OutputFormat = "TextOutputFormat",
            Compressed = true,
            NumBuckets = 8,
            BucketColumns = ["id"],
            SortColumns = [],
            Parameters = new Dictionary<string, string> { ["p"] = "q" },
            SkewedInfo = new SkewedInfo(),
            SerDeInfo = new SerDeInfo
            {
                SerializationLib = "LazySimpleSerDe",
                Parameters = new Dictionary<string, string>()
            }
        };

        var roundTripped = original.ToDto().ToModel();

        Assert.AreEqual("hdfs:///data/table", roundTripped.Location);
        Assert.IsTrue(roundTripped.Compressed);
        Assert.AreEqual(8, roundTripped.NumBuckets);
        Assert.AreEqual("q", roundTripped.Parameters["p"]);
    }

    // ── HiveTable ─────────────────────────────────────────────────────────────

    [TestMethod]
    public void TableRequest_ToModel_SeparatesDataAndPartitionColumns()
    {
        var request = new TableRequest(
            Name: "events",
            DatabaseName: "analytics",
            Owner: "pipeline",
            TableType: "ManagedTable",
            StorageDescriptor: new StorageDescriptorDto(
                "hdfs:///events", "TextIF", "TextOF", false, -1,
                new SerDeInfoDto(null, "LazySimpleSerDe", null),
                null, null, null),
            Columns: [new ColumnDto("event_id", "bigint", null, 0, false),
                      new ColumnDto("payload",  "string", null, 1, false)],
            PartitionKeys: [new ColumnDto("dt", "string", null, 0, false)],
            ViewOriginalText: null,
            ViewExpandedText: null,
            Parameters: null);

        var model = request.ToModel();

        var dataCols = model.Columns.Where(c => !c.IsPartitionKey).ToList();
        var partCols = model.Columns.Where(c => c.IsPartitionKey).ToList();

        Assert.AreEqual(2, dataCols.Count);
        Assert.AreEqual(1, partCols.Count);
        Assert.AreEqual("dt", partCols[0].Name);
    }

    [TestMethod]
    public void HiveTable_ToDto_SeparatesDataAndPartitionColumnsInDto()
    {
        var table = new HiveTable
        {
            Name = "events",
            Database = new HiveDatabase { Name = "analytics" },
            Owner = "pipeline",
            TableType = TableType.ManagedTable,
            StorageDescriptor = new StorageDescriptor
            {
                Location = "hdfs:///events",
                InputFormat = "TextIF", OutputFormat = "TextOF",
                BucketColumns = [], SortColumns = [],
                Parameters = new Dictionary<string, string>(),
                SkewedInfo = new SkewedInfo(),
                SerDeInfo = new SerDeInfo { SerializationLib = "LazySimple", Parameters = new() }
            },
            Columns = new List<HiveColumn>
            {
                new() { Name = "id",   TypeName = "int",    OrdinalPosition = 0, IsPartitionKey = false },
                new() { Name = "name", TypeName = "string", OrdinalPosition = 1, IsPartitionKey = false },
                new() { Name = "dt",   TypeName = "string", OrdinalPosition = 0, IsPartitionKey = true  },
            },
            Parameters = new Dictionary<string, string>()
        };

        var dto = table.ToDto();

        Assert.AreEqual(2, dto.Columns.Count);
        Assert.AreEqual(1, dto.PartitionKeys.Count);
        Assert.AreEqual("dt", dto.PartitionKeys[0].Name);
        Assert.IsTrue(dto.Columns.All(c => !c.IsPartitionKey));
    }

    // ── Partition ─────────────────────────────────────────────────────────────

    [TestMethod]
    public void BuildPartitionName_ReturnsCorrectFormat()
    {
        var keys = new List<HiveColumn>
        {
            new() { Name = "year",  OrdinalPosition = 0 },
            new() { Name = "month", OrdinalPosition = 1 },
            new() { Name = "day",   OrdinalPosition = 2 },
        };
        var values = new List<string> { "2024", "01", "15" };

        var name = MetastoreMapper.BuildPartitionName(keys, values);

        Assert.AreEqual("year=2024/month=01/day=15", name);
    }

    [TestMethod]
    public void BuildPartitionName_FallsBackToSlashJoin_WhenCountMismatch()
    {
        var keys = new List<HiveColumn> { new() { Name = "dt" } };
        var values = new List<string> { "2024", "01" }; // 2 values, 1 key

        var name = MetastoreMapper.BuildPartitionName(keys, values);

        Assert.AreEqual("2024/01", name);
    }

    [TestMethod]
    public void PartitionRequest_ToModel_MapsValues()
    {
        var request = new PartitionRequest(
            Values: ["2024", "06"],
            StorageDescriptor: null,
            Parameters: new Dictionary<string, string> { ["numRows"] = "500" });

        var model = request.ToModel();

        CollectionAssert.AreEqual(new[] { "2024", "06" }, model.Values);
        Assert.AreEqual("500", model.Parameters["numRows"]);
    }

    // ── ColumnStatistics ──────────────────────────────────────────────────────

    [TestMethod]
    public void ColumnStatisticsDto_RoundTrip_PreservesAllFields()
    {
        var original = new ColumnStatistics
        {
            ColumnName = "amount",
            ColumnType = "decimal(18,2)",
            StatisticsType = StatisticsType.Decimal,
            NumNulls = 10,
            NumDistinctValues = 5000,
            DecimalLow = "0.01",
            DecimalHigh = "9999.99",
            BitVector = "base64encodedvector"
        };

        var dto = original.ToDto();
        var roundTripped = dto.ToModel();

        Assert.AreEqual("amount", roundTripped.ColumnName);
        Assert.AreEqual(StatisticsType.Decimal, roundTripped.StatisticsType);
        Assert.AreEqual(10L, roundTripped.NumNulls);
        Assert.AreEqual("0.01", roundTripped.DecimalLow);
        Assert.AreEqual("9999.99", roundTripped.DecimalHigh);
        Assert.AreEqual("base64encodedvector", roundTripped.BitVector);
    }
}
