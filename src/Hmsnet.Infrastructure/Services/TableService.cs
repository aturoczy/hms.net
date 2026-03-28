using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace Hmsnet.Infrastructure.Services;

public class TableService(MetastoreDbContext db) : ITableService
{
    private IQueryable<HiveTable> TablesWithDetails =>
        db.Tables
            .Include(t => t.Database)
            .Include(t => t.Columns)
            .Include(t => t.StorageDescriptor)
                .ThenInclude(sd => sd.SerDeInfo);

    public async Task<IReadOnlyList<string>> GetAllTableNamesAsync(string dbName, CancellationToken ct = default) =>
        await db.Tables
            .Where(t => t.Database.Name == dbName.ToLowerInvariant())
            .Select(t => t.Name)
            .OrderBy(n => n)
            .ToListAsync(ct);

    public async Task<IReadOnlyList<string>> GetTableNamesLikeAsync(string dbName, string pattern, CancellationToken ct = default)
    {
        var regex = SqlLikeToEfLike(pattern);
        return await db.Tables
            .Where(t => t.Database.Name == dbName.ToLowerInvariant() && EF.Functions.Like(t.Name, regex))
            .Select(t => t.Name)
            .OrderBy(n => n)
            .ToListAsync(ct);
    }

    public async Task<IReadOnlyList<HiveTable>> GetAllTablesAsync(string dbName, CancellationToken ct = default) =>
        await TablesWithDetails
            .Where(t => t.Database.Name == dbName.ToLowerInvariant())
            .OrderBy(t => t.Name)
            .ToListAsync(ct);

    public async Task<HiveTable?> GetTableAsync(string dbName, string tableName, CancellationToken ct = default) =>
        await TablesWithDetails
            .FirstOrDefaultAsync(t =>
                t.Database.Name == dbName.ToLowerInvariant() &&
                t.Name == tableName.ToLowerInvariant(), ct);

    public async Task<IReadOnlyList<HiveTable>> GetTablesAsync(string dbName, IEnumerable<string> tableNames, CancellationToken ct = default)
    {
        var names = tableNames.Select(n => n.ToLowerInvariant()).ToList();
        return await TablesWithDetails
            .Where(t => t.Database.Name == dbName.ToLowerInvariant() && names.Contains(t.Name))
            .ToListAsync(ct);
    }

    public async Task<HiveTable> CreateTableAsync(HiveTable table, CancellationToken ct = default)
    {
        var database = await db.Databases.FirstOrDefaultAsync(d => d.Name == table.Database.Name.ToLowerInvariant(), ct)
            ?? throw new NoSuchObjectException($"Database '{table.Database.Name}' does not exist.");

        table.Name = table.Name.ToLowerInvariant();
        table.DatabaseId = database.Id;
        table.Database = database;

        if (await db.Tables.AnyAsync(t => t.DatabaseId == database.Id && t.Name == table.Name, ct))
            throw new AlreadyExistsException($"Table '{database.Name}.{table.Name}' already exists.");

        if (string.IsNullOrWhiteSpace(table.StorageDescriptor.Location))
            table.StorageDescriptor.Location = $"{database.LocationUri}/{table.Name}";

        // Columns already carry correct IsPartitionKey flags — nothing to merge.

        db.Tables.Add(table);
        await db.SaveChangesAsync(ct);
        return table;
    }

    public async Task<HiveTable> AlterTableAsync(string dbName, string tableName, HiveTable updated, CancellationToken ct = default)
    {
        var existing = await TablesWithDetails.FirstOrDefaultAsync(t =>
            t.Database.Name == dbName.ToLowerInvariant() && t.Name == tableName.ToLowerInvariant(), ct)
            ?? throw new NoSuchObjectException($"Table '{dbName}.{tableName}' does not exist.");

        existing.Name = updated.Name.ToLowerInvariant();
        existing.Owner = updated.Owner;
        existing.TableType = updated.TableType;
        existing.Parameters = updated.Parameters;
        existing.ViewOriginalText = updated.ViewOriginalText;
        existing.ViewExpandedText = updated.ViewExpandedText;
        existing.Retention = updated.Retention;
        existing.LastAccessTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        // Update StorageDescriptor
        existing.StorageDescriptor.Location = updated.StorageDescriptor.Location;
        existing.StorageDescriptor.InputFormat = updated.StorageDescriptor.InputFormat;
        existing.StorageDescriptor.OutputFormat = updated.StorageDescriptor.OutputFormat;
        existing.StorageDescriptor.Compressed = updated.StorageDescriptor.Compressed;
        existing.StorageDescriptor.NumBuckets = updated.StorageDescriptor.NumBuckets;
        existing.StorageDescriptor.BucketColumns = updated.StorageDescriptor.BucketColumns;
        existing.StorageDescriptor.SortColumns = updated.StorageDescriptor.SortColumns;
        existing.StorageDescriptor.Parameters = updated.StorageDescriptor.Parameters;
        existing.StorageDescriptor.SerDeInfo.Name = updated.StorageDescriptor.SerDeInfo.Name;
        existing.StorageDescriptor.SerDeInfo.SerializationLib = updated.StorageDescriptor.SerDeInfo.SerializationLib;
        existing.StorageDescriptor.SerDeInfo.Parameters = updated.StorageDescriptor.SerDeInfo.Parameters;

        // Replace columns
        db.Columns.RemoveRange(existing.Columns);
        var newCols = updated.Columns.Concat(updated.PartitionKeys).ToList();
        foreach (var col in newCols) col.TableId = existing.Id;
        db.Columns.AddRange(newCols);

        await db.SaveChangesAsync(ct);
        return existing;
    }

    public async Task DropTableAsync(string dbName, string tableName, bool deleteData, CancellationToken ct = default)
    {
        var table = await db.Tables
            .Include(t => t.StorageDescriptor)
            .FirstOrDefaultAsync(t =>
                t.Database.Name == dbName.ToLowerInvariant() &&
                t.Name == tableName.ToLowerInvariant(), ct)
            ?? throw new NoSuchObjectException($"Table '{dbName}.{tableName}' does not exist.");

        db.Tables.Remove(table);
        await db.SaveChangesAsync(ct);
        // Note: actual data deletion (HDFS/S3) is left to the caller / storage layer
    }

    public async Task<bool> TableExistsAsync(string dbName, string tableName, CancellationToken ct = default) =>
        await db.Tables.AnyAsync(t =>
            t.Database.Name == dbName.ToLowerInvariant() &&
            t.Name == tableName.ToLowerInvariant(), ct);

    public async Task<IReadOnlyList<HiveColumn>> GetFieldsAsync(string dbName, string tableName, CancellationToken ct = default)
    {
        var table = await GetTableAsync(dbName, tableName, ct)
            ?? throw new NoSuchObjectException($"Table '{dbName}.{tableName}' does not exist.");
        return table.Columns.Where(c => !c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
    }

    public async Task<IReadOnlyList<HiveColumn>> GetSchemaAsync(string dbName, string tableName, CancellationToken ct = default)
    {
        var table = await GetTableAsync(dbName, tableName, ct)
            ?? throw new NoSuchObjectException($"Table '{dbName}.{tableName}' does not exist.");
        return table.Columns.OrderBy(c => c.IsPartitionKey).ThenBy(c => c.OrdinalPosition).ToList();
    }

    private static string SqlLikeToEfLike(string hivePattern) =>
        hivePattern.Replace('*', '%').Replace('?', '_');
}
