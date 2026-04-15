using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Data;

namespace Hmsnet.Iceberg.Tests.Helpers;

public static class IcebergSeedData
{
    public static readonly string DefaultMetadataJson = """
        {
            "format-version": 2,
            "table-uuid": "test-uuid-1234",
            "location": "/testdb/testtable",
            "last-sequence-number": 0,
            "last-updated-ms": 1700000000000,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "name", "required": false, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": []
        }
        """;

    public static async Task<(HiveDatabase Database, HiveTable Table, IcebergTableMetadata IcebergMeta)>
        SeedIcebergTableAsync(
            MetastoreDbContext ctx,
            string dbName = "testdb",
            string tableName = "testtable")
    {
        var database = new HiveDatabase
        {
            Name = dbName,
            LocationUri = $"hdfs:///user/hive/warehouse/{dbName}.db",
            Parameters = []
        };
        ctx.Databases.Add(database);
        await ctx.SaveChangesAsync();

        var table = new HiveTable
        {
            Name = tableName,
            DatabaseId = database.Id,
            Database = database,
            TableType = TableType.ExternalTable,
            Parameters = new Dictionary<string, string> { ["table_type"] = "ICEBERG" },
            StorageDescriptor = new StorageDescriptor
            {
                Location = $"/{dbName}/{tableName}",
                InputFormat = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
                OutputFormat = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
                SerDeInfo = new SerDeInfo { SerializationLib = "org.apache.iceberg.mr.hive.HiveIcebergSerDe" }
            },
            Columns =
            [
                new HiveColumn { Name = "id",   TypeName = "long",   OrdinalPosition = 0, IsPartitionKey = false },
                new HiveColumn { Name = "name",  TypeName = "string", OrdinalPosition = 1, IsPartitionKey = false }
            ]
        };
        ctx.Tables.Add(table);
        await ctx.SaveChangesAsync();

        var iceMeta = new IcebergTableMetadata
        {
            HiveTableId = table.Id,
            MetadataLocation = $"/{dbName}/{tableName}/metadata/v1.metadata.json",
            MetadataJson = DefaultMetadataJson
        };
        ctx.IcebergMetadata.Add(iceMeta);
        await ctx.SaveChangesAsync();

        return (database, table, iceMeta);
    }
}
