using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace Hmsnet.Infrastructure.Services;

public class IcebergCatalogService(IDatabaseService dbSvc, ITableService tableSvc, MetastoreDbContext db)
    : IIcebergCatalogService
{
    // ── Namespace operations ──────────────────────────────────────────────────

    public Task<IReadOnlyList<HiveDatabase>> ListNamespacesAsync(CancellationToken ct = default) =>
        dbSvc.GetAllDatabasesAsync(ct);

    public Task<HiveDatabase> CreateNamespaceAsync(string name, Dictionary<string, string> properties, CancellationToken ct = default) =>
        dbSvc.CreateDatabaseAsync(new HiveDatabase { Name = name, Parameters = properties }, ct);

    public Task<HiveDatabase?> GetNamespaceAsync(string name, CancellationToken ct = default) =>
        dbSvc.GetDatabaseAsync(name, ct);

    public async Task<(List<string> Updated, List<string> Removed)> UpdateNamespacePropertiesAsync(
        string name, List<string> removals, Dictionary<string, string> updates, CancellationToken ct = default)
    {
        var db2 = await dbSvc.GetDatabaseAsync(name, ct)
            ?? throw new NoSuchObjectException($"Namespace '{name}' does not exist.");

        var updated = new List<string>();
        var removed = new List<string>();

        foreach (var key in removals)
        {
            if (db2.Parameters.Remove(key))
                removed.Add(key);
        }

        foreach (var (key, value) in updates)
        {
            db2.Parameters[key] = value;
            updated.Add(key);
        }

        await dbSvc.AlterDatabaseAsync(name, db2, ct);
        return (updated, removed);
    }

    public Task DropNamespaceAsync(string name, CancellationToken ct = default) =>
        dbSvc.DropDatabaseAsync(name, cascade: false, ct);

    // ── Table operations ──────────────────────────────────────────────────────

    public Task<IReadOnlyList<HiveTable>> ListTablesAsync(string dbName, CancellationToken ct = default) =>
        tableSvc.GetAllTablesAsync(dbName, ct);

    public async Task<IcebergTableMetadata> CreateTableAsync(
        string dbName, HiveTable table, string metadataLocation, string metadataJson, CancellationToken ct = default)
    {
        var created = await tableSvc.CreateTableAsync(table, ct);
        var meta = new IcebergTableMetadata
        {
            HiveTableId = created.Id,
            MetadataLocation = metadataLocation,
            MetadataJson = metadataJson
        };
        db.IcebergMetadata.Add(meta);
        await db.SaveChangesAsync(ct);
        meta.HiveTable = created;
        return meta;
    }

    public async Task<IcebergTableMetadata?> LoadTableAsync(string dbName, string tableName, CancellationToken ct = default)
    {
        var table = await tableSvc.GetTableAsync(dbName, tableName, ct);
        if (table is null) return null;

        return await db.IcebergMetadata
            .Include(m => m.HiveTable)
            .FirstOrDefaultAsync(m => m.HiveTableId == table.Id, ct);
    }

    public async Task<IcebergTableMetadata> CommitTableAsync(
        string dbName, string tableName, string newMetadataLocation, string newMetadataJson, CancellationToken ct = default)
    {
        var table = await tableSvc.GetTableAsync(dbName, tableName, ct)
            ?? throw new NoSuchObjectException($"Table '{dbName}.{tableName}' does not exist.");

        var meta = await db.IcebergMetadata
            .Include(m => m.HiveTable)
            .FirstOrDefaultAsync(m => m.HiveTableId == table.Id, ct)
            ?? throw new NoSuchObjectException($"Iceberg metadata for '{dbName}.{tableName}' does not exist.");

        meta.MetadataLocation = newMetadataLocation;
        meta.MetadataJson = newMetadataJson;
        await db.SaveChangesAsync(ct);
        return meta;
    }

    public Task DropTableAsync(string dbName, string tableName, bool purge, CancellationToken ct = default) =>
        tableSvc.DropTableAsync(dbName, tableName, purge, ct);
    // IcebergTableMetadata is cascade-deleted via FK when HiveTable is dropped.

    public async Task<IcebergTableMetadata> RegisterTableAsync(
        string dbName, string tableName, string metadataLocation, string metadataJson, CancellationToken ct = default)
    {
        // Ensure the database exists
        if (!await dbSvc.DatabaseExistsAsync(dbName, ct))
            throw new NoSuchObjectException($"Namespace '{dbName}' does not exist.");

        // Create a minimal HiveTable for the registered table
        var table = new HiveTable
        {
            Name = tableName,
            TableType = TableType.ExternalTable,
            Parameters = new Dictionary<string, string> { ["table_type"] = "ICEBERG" },
            StorageDescriptor = new StorageDescriptor(),
            Database = new HiveDatabase { Name = dbName }
        };

        return await CreateTableAsync(dbName, table, metadataLocation, metadataJson, ct);
    }

    public async Task RenameTableAsync(
        string fromDb, string fromTable, string toDb, string toTable, CancellationToken ct = default)
    {
        var table = await tableSvc.GetTableAsync(fromDb, fromTable, ct)
            ?? throw new NoSuchObjectException($"Table '{fromDb}.{fromTable}' does not exist.");

        // If moving to a different database, update the database reference
        if (!string.Equals(fromDb, toDb, StringComparison.OrdinalIgnoreCase))
        {
            var targetDb = await dbSvc.GetDatabaseAsync(toDb, ct)
                ?? throw new NoSuchObjectException($"Namespace '{toDb}' does not exist.");
            table.DatabaseId = targetDb.Id;
            table.Database = targetDb;
        }

        // Clone columns without EF-tracked IDs so AlterTableAsync can re-insert them cleanly.
        var clonedColumns = table.Columns.Select(c => new HiveColumn
        {
            Name = c.Name,
            TypeName = c.TypeName,
            Comment = c.Comment,
            OrdinalPosition = c.OrdinalPosition,
            IsPartitionKey = c.IsPartitionKey
        }).ToList();

        var updated = new HiveTable
        {
            Name = toTable,
            Owner = table.Owner,
            TableType = table.TableType,
            Parameters = table.Parameters,
            Retention = table.Retention,
            ViewOriginalText = table.ViewOriginalText,
            ViewExpandedText = table.ViewExpandedText,
            StorageDescriptor = table.StorageDescriptor,
            Columns = clonedColumns,
            Database = table.Database
        };

        await tableSvc.AlterTableAsync(fromDb, fromTable, updated, ct);
    }
}
