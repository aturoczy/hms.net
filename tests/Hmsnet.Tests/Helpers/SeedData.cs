using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Data;

namespace Hmsnet.Tests.Helpers;

/// <summary>Helper methods that insert common entities into the in-memory DB.</summary>
public static class SeedData
{
    public static HiveDatabase Database(string name = "testdb") => new()
    {
        Name = name,
        LocationUri = $"hdfs:///user/hive/warehouse/{name}.db",
        OwnerName = "test_user",
        Parameters = new Dictionary<string, string>()
    };

    public static HiveTable Table(
        int databaseId,
        string dbName = "testdb",
        string tableName = "testtable",
        IList<HiveColumn>? columns = null,
        IList<HiveColumn>? partitionKeys = null)
    {
        var allCols = new List<HiveColumn>();
        allCols.AddRange(columns ?? DefaultColumns());
        allCols.AddRange(partitionKeys ?? Array.Empty<HiveColumn>());

        return new HiveTable
        {
            Name = tableName,
            DatabaseId = databaseId,
            Database = new HiveDatabase { Id = databaseId, Name = dbName },
            Owner = "test_user",
            TableType = TableType.ManagedTable,
            Columns = allCols,
            Parameters = new Dictionary<string, string>(),
            StorageDescriptor = DefaultSd($"hdfs:///user/hive/warehouse/{dbName}.db/{tableName}"),
        };
    }

    public static List<HiveColumn> DefaultColumns() =>
    [
        new HiveColumn { Name = "id",   TypeName = "int",    OrdinalPosition = 0, IsPartitionKey = false },
        new HiveColumn { Name = "name", TypeName = "string", OrdinalPosition = 1, IsPartitionKey = false },
    ];

    public static List<HiveColumn> DefaultPartitionKeys() =>
    [
        new HiveColumn { Name = "dt", TypeName = "string", OrdinalPosition = 0, IsPartitionKey = true },
    ];

    public static StorageDescriptor DefaultSd(string location = "hdfs:///data") => new()
    {
        Location = location,
        InputFormat  = "org.apache.hadoop.mapred.TextInputFormat",
        OutputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        Compressed   = false,
        NumBuckets   = -1,
        BucketColumns = [],
        SortColumns   = [],
        Parameters    = new Dictionary<string, string>(),
        SkewedInfo    = new SkewedInfo(),
        SerDeInfo = new SerDeInfo
        {
            SerializationLib = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            Parameters = new Dictionary<string, string> { ["field.delim"] = "," }
        }
    };

    public static HivePartition Partition(int tableId, IList<string> values) => new()
    {
        TableId = tableId,
        Values = values.ToList(),
        Parameters = new Dictionary<string, string>(),
        StorageDescriptor = DefaultSd()
    };

    /// <summary>
    /// Inserts a <see cref="HiveDatabase"/> and saves it, returning the tracked entity with its assigned Id.
    /// </summary>
    public static async Task<HiveDatabase> SeedDatabaseAsync(MetastoreDbContext db, string name = "testdb")
    {
        var entity = Database(name);
        db.Databases.Add(entity);
        await db.SaveChangesAsync();
        return entity;
    }

    /// <summary>
    /// Inserts a database + table, returns the tracked table.
    /// </summary>
    public static async Task<(HiveDatabase db, HiveTable table)> SeedTableAsync(
        MetastoreDbContext ctx,
        string dbName = "testdb",
        string tableName = "testtable",
        IList<HiveColumn>? extraPartKeys = null)
    {
        var database = await SeedDatabaseAsync(ctx, dbName);
        var partKeys = extraPartKeys ?? Array.Empty<HiveColumn>();

        var allCols = new List<HiveColumn>(DefaultColumns());
        foreach (var pk in partKeys) allCols.Add(pk);

        var table = new HiveTable
        {
            Name = tableName,
            DatabaseId = database.Id,
            Database = database,
            Owner = "test_user",
            TableType = TableType.ManagedTable,
            Columns = allCols,
            Parameters = new Dictionary<string, string>(),
            StorageDescriptor = DefaultSd($"hdfs:///user/hive/warehouse/{dbName}.db/{tableName}")
        };
        ctx.Tables.Add(table);
        await ctx.SaveChangesAsync();
        return (database, table);
    }
}
