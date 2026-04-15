using Hmsnet.Core.Features.Databases.Commands;
using Hmsnet.Core.Features.Databases.Queries;
using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Core.Features.Tables.Commands;
using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Api.Thrift;

/// <summary>
/// Implements the ThriftHiveMetastore service operations.
/// This handler is called by the generated HiveMetastoreProcessor.
/// All types here mirror the Hive Metastore Thrift IDL structs, but are
/// represented as plain C# classes (generated Thrift types would normally
/// come from running thrift --gen csharp hive_metastore.thrift).
/// </summary>
public class ThriftHmsHandler(ISender sender, ILogger<ThriftHmsHandler> logger)
{
    // ── Database operations ───────────────────────────────────────────────────

    public async Task CreateDatabaseAsync(ThriftDatabase db, CancellationToken ct)
    {
        logger.LogDebug("Thrift: create_database {Name}", db.Name);
        await sender.Send(new CreateDatabaseCommand(new HiveDatabase
        {
            Name = db.Name,
            Description = db.Description,
            LocationUri = db.LocationUri ?? $"hdfs:///user/hive/warehouse/{db.Name}.db",
            OwnerName = db.OwnerName,
            Parameters = db.Parameters ?? []
        }), ct);
    }

    public async Task<ThriftDatabase?> GetDatabaseAsync(string name, CancellationToken ct)
    {
        logger.LogDebug("Thrift: get_database {Name}", name);
        var db = await sender.Send(new GetDatabaseQuery(name), ct);
        if (db is null) return null;
        return new ThriftDatabase(db.Name, db.Description, db.LocationUri, db.OwnerName, db.Parameters);
    }

    public async Task<IReadOnlyList<string>> GetAllDatabasesAsync(CancellationToken ct)
    {
        logger.LogDebug("Thrift: get_all_databases");
        return await sender.Send(new GetAllDatabaseNamesQuery(), ct);
    }

    public async Task DropDatabaseAsync(string name, bool deleteData, bool cascade, CancellationToken ct)
    {
        logger.LogDebug("Thrift: drop_database {Name} cascade={Cascade}", name, cascade);
        await sender.Send(new DropDatabaseCommand(name, cascade), ct);
    }

    // ── Table operations ──────────────────────────────────────────────────────

    public async Task CreateTableAsync(ThriftTable table, CancellationToken ct)
    {
        logger.LogDebug("Thrift: create_table {Db}.{Table}", table.DbName, table.TableName);
        var model = MapThriftTable(table);
        model.Database = new HiveDatabase { Name = table.DbName };
        await sender.Send(new CreateTableCommand(model), ct);
    }

    public async Task<ThriftTable?> GetTableAsync(string dbName, string tableName, CancellationToken ct)
    {
        logger.LogDebug("Thrift: get_table {Db}.{Table}", dbName, tableName);
        var t = await sender.Send(new GetTableQuery(dbName, tableName), ct);
        return t is null ? null : MapToThriftTable(t);
    }

    public async Task<IReadOnlyList<string>> GetAllTablesAsync(string dbName, CancellationToken ct)
    {
        logger.LogDebug("Thrift: get_all_tables {Db}", dbName);
        return await sender.Send(new GetAllTableNamesQuery(dbName), ct);
    }

    public async Task<IReadOnlyList<string>> GetTablesAsync(string dbName, string pattern, CancellationToken ct)
    {
        logger.LogDebug("Thrift: get_tables {Db} pattern={Pattern}", dbName, pattern);
        return await sender.Send(new GetTableNamesLikeQuery(dbName, pattern), ct);
    }

    public async Task DropTableAsync(string dbName, string tableName, bool deleteData, CancellationToken ct)
    {
        logger.LogDebug("Thrift: drop_table {Db}.{Table}", dbName, tableName);
        await sender.Send(new DropTableCommand(dbName, tableName, deleteData), ct);
    }

    public async Task AlterTableAsync(string dbName, string tableName, ThriftTable newTable, CancellationToken ct)
    {
        logger.LogDebug("Thrift: alter_table {Db}.{Table}", dbName, tableName);
        await sender.Send(new AlterTableCommand(dbName, tableName, MapThriftTable(newTable)), ct);
    }

    // ── Schema operations ─────────────────────────────────────────────────────

    public async Task<IReadOnlyList<ThriftFieldSchema>> GetFieldsAsync(string dbName, string tableName, CancellationToken ct)
    {
        var cols = await sender.Send(new GetFieldsQuery(dbName, tableName), ct);
        return cols.Select(MapToThriftField).ToList();
    }

    public async Task<IReadOnlyList<ThriftFieldSchema>> GetSchemaAsync(string dbName, string tableName, CancellationToken ct)
    {
        var cols = await sender.Send(new GetSchemaQuery(dbName, tableName), ct);
        return cols.Select(MapToThriftField).ToList();
    }

    // ── Partition operations ──────────────────────────────────────────────────

    public async Task<ThriftPartition> AddPartitionAsync(ThriftPartition partition, CancellationToken ct)
    {
        logger.LogDebug("Thrift: add_partition {Db}.{Table}", partition.DbName, partition.TableName);
        var model = MapThriftPartition(partition);
        var result = await sender.Send(new AddPartitionCommand(partition.DbName, partition.TableName, model), ct);
        partition.CreateTime = (int)result.Partition.CreateTime;
        return partition;
    }

    public async Task<IReadOnlyList<ThriftPartition>> AddPartitionsAsync(IList<ThriftPartition> partitions, CancellationToken ct)
    {
        var results = new List<ThriftPartition>();
        foreach (var p in partitions)
            results.Add(await AddPartitionAsync(p, ct));
        return results;
    }

    public async Task<ThriftPartition?> GetPartitionAsync(string dbName, string tableName, IList<string> values, CancellationToken ct)
    {
        var result = await sender.Send(new GetPartitionByValuesQuery(dbName, tableName, values.ToList()), ct);
        return result is null ? null : MapToThriftPartition(result.Partition, dbName, tableName);
    }

    public async Task<IReadOnlyList<ThriftPartition>> GetPartitionsAsync(string dbName, string tableName, int maxParts, CancellationToken ct)
    {
        var result = await sender.Send(new GetPartitionsQuery(dbName, tableName, maxParts), ct);
        return result.Partitions.Select(p => MapToThriftPartition(p, dbName, tableName)).ToList();
    }

    public async Task<IReadOnlyList<string>> GetPartitionNamesAsync(string dbName, string tableName, int maxParts, CancellationToken ct) =>
        await sender.Send(new GetPartitionNamesQuery(dbName, tableName, maxParts), ct);

    public async Task<IReadOnlyList<ThriftPartition>> GetPartitionsByFilterAsync(string dbName, string tableName, string filter, int maxParts, CancellationToken ct)
    {
        var result = await sender.Send(new GetPartitionsByFilterQuery(dbName, tableName, filter, maxParts), ct);
        return result.Partitions.Select(p => MapToThriftPartition(p, dbName, tableName)).ToList();
    }

    public async Task<bool> DropPartitionAsync(string dbName, string tableName, IList<string> values, bool deleteData, CancellationToken ct) =>
        await sender.Send(new DropPartitionCommand(dbName, tableName, values.ToList(), deleteData), ct);

    public async Task AlterPartitionAsync(string dbName, string tableName, ThriftPartition partition, CancellationToken ct) =>
        await sender.Send(new AlterPartitionCommand(dbName, tableName, MapThriftPartition(partition)), ct);

    // ── Mapping helpers ───────────────────────────────────────────────────────

    private static HiveTable MapThriftTable(ThriftTable t)
    {
        // Data columns come from SD.cols (field 1 of StorageDescriptor) per HMS wire protocol.
        // Fall back to t.Columns if the client sent them there.
        var sdCols = t.Sd?.Cols ?? t.Columns ?? [];
        var dataCols = sdCols.Select((f, i) => MapThriftField(f, i)).ToList();
        var partCols = t.PartitionKeys?.Select((f, i) => MapThriftField(f, i, true)).ToList() ?? [];
        return new HiveTable
        {
            Name = t.TableName,
            Owner = t.Owner,
            TableType = Enum.TryParse<TableType>(t.TableType, true, out var tt) ? tt : TableType.ManagedTable,
            ViewOriginalText = t.ViewOriginalText,
            ViewExpandedText = t.ViewExpandedText,
            Parameters = t.Parameters ?? [],
            Retention = t.Retention,
            StorageDescriptor = t.Sd is null ? new StorageDescriptor() : MapThriftSd(t.Sd),
            Columns = [.. dataCols, .. partCols]
        };
    }

    private static ThriftTable MapToThriftTable(HiveTable t)
    {
        var dataCols = t.Columns.Where(c => !c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).Select(MapToThriftField).ToList();
        var partKeys = t.PartitionKeys.OrderBy(c => c.OrdinalPosition).Select(MapToThriftField).ToList();
        return new ThriftTable(
            t.Name,
            t.Database?.Name ?? string.Empty,
            t.Owner,
            TableTypeToString(t.TableType),
            (int)t.CreateTime,
            (int)t.LastAccessTime,
            t.Retention,
            MapToThriftSdWithCols(t.StorageDescriptor, dataCols),
            dataCols,
            partKeys,
            t.ViewOriginalText,
            t.ViewExpandedText,
            t.Parameters);
    }

    private static string TableTypeToString(TableType tt) => tt switch
    {
        TableType.ManagedTable => "MANAGED_TABLE",
        TableType.ExternalTable => "EXTERNAL_TABLE",
        TableType.VirtualView => "VIRTUAL_VIEW",
        TableType.MaterializedView => "MATERIALIZED_VIEW",
        _ => tt.ToString().ToUpperInvariant()
    };

    private static HiveColumn MapThriftField(ThriftFieldSchema f, int pos, bool isPartKey = false) => new()
    {
        Name = f.Name,
        TypeName = f.Type,
        Comment = f.Comment,
        OrdinalPosition = pos,
        IsPartitionKey = isPartKey
    };

    private static ThriftFieldSchema MapToThriftField(HiveColumn c) => new(c.Name, c.TypeName, c.Comment);

    private static StorageDescriptor MapThriftSd(ThriftStorageDescriptor sd) => new()
    {
        Location = sd.Location ?? string.Empty,
        InputFormat = sd.InputFormat ?? string.Empty,
        OutputFormat = sd.OutputFormat ?? string.Empty,
        Compressed = sd.Compressed,
        NumBuckets = sd.NumBuckets,
        BucketColumns = sd.BucketCols ?? [],
        SortColumns = sd.SortCols?.Select(s => new SortOrder { Column = s.Col, Order = (SortDirection)s.Order }).ToList() ?? [],
        Parameters = sd.Parameters ?? [],
        SerDeInfo = sd.SerDeInfo is null ? new SerDeInfo() : new SerDeInfo
        {
            Name = sd.SerDeInfo.Name,
            SerializationLib = sd.SerDeInfo.SerializationLib ?? string.Empty,
            Parameters = sd.SerDeInfo.Parameters ?? []
        }
    };

    private static ThriftStorageDescriptor MapToThriftSd(StorageDescriptor sd) => new(
        sd.Location, sd.InputFormat, sd.OutputFormat, sd.Compressed, sd.NumBuckets,
        new ThriftSerDeInfo(sd.SerDeInfo.Name, sd.SerDeInfo.SerializationLib, sd.SerDeInfo.Parameters),
        sd.BucketColumns,
        sd.SortColumns.Select(s => new ThriftOrder(s.Column, (int)s.Order)).ToList(),
        sd.Parameters);

    private static ThriftStorageDescriptor MapToThriftSdWithCols(StorageDescriptor sd, List<ThriftFieldSchema> cols) => new(
        sd.Location, sd.InputFormat, sd.OutputFormat, sd.Compressed, sd.NumBuckets,
        new ThriftSerDeInfo(sd.SerDeInfo.Name, sd.SerDeInfo.SerializationLib, sd.SerDeInfo.Parameters),
        sd.BucketColumns,
        sd.SortColumns.Select(s => new ThriftOrder(s.Column, (int)s.Order)).ToList(),
        sd.Parameters,
        cols);

    private static HivePartition MapThriftPartition(ThriftPartition p) => new()
    {
        Values = p.Values?.ToList() ?? [],
        Parameters = p.Parameters ?? [],
        StorageDescriptor = p.Sd is null ? new StorageDescriptor() : MapThriftSd(p.Sd)
    };

    private static ThriftPartition MapToThriftPartition(HivePartition p, string dbName, string tableName) => new(
        p.Values,
        dbName,
        tableName,
        (int)p.CreateTime,
        (int)p.LastAccessTime,
        p.StorageDescriptor is null ? null : MapToThriftSd(p.StorageDescriptor),
        p.Parameters);
}
