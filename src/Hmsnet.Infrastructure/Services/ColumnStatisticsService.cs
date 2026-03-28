using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace Hmsnet.Infrastructure.Services;

public class ColumnStatisticsService(MetastoreDbContext db) : IColumnStatisticsService
{
    public async Task<IReadOnlyList<ColumnStatistics>> GetTableColumnStatisticsAsync(
        string dbName, string tableName, IEnumerable<string> columnNames, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);
        var cols = columnNames.ToList();

        return await db.ColumnStatistics
            .Where(cs => cs.TableId == table.Id && cols.Contains(cs.ColumnName))
            .ToListAsync(ct);
    }

    public async Task UpdateTableColumnStatisticsAsync(
        string dbName, string tableName, IEnumerable<ColumnStatistics> stats, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);

        foreach (var stat in stats)
        {
            var existing = await db.ColumnStatistics
                .FirstOrDefaultAsync(cs => cs.TableId == table.Id && cs.ColumnName == stat.ColumnName, ct);

            if (existing is not null)
            {
                CopyStats(stat, existing);
                existing.LastAnalyzed = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            }
            else
            {
                stat.TableId = table.Id;
                stat.PartitionId = null;
                stat.LastAnalyzed = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                db.ColumnStatistics.Add(stat);
            }
        }

        await db.SaveChangesAsync(ct);
    }

    public async Task DeleteTableColumnStatisticsAsync(
        string dbName, string tableName, string? columnName, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);

        var q = db.ColumnStatistics.Where(cs => cs.TableId == table.Id);
        if (columnName is not null)
            q = q.Where(cs => cs.ColumnName == columnName);

        db.ColumnStatistics.RemoveRange(await q.ToListAsync(ct));
        await db.SaveChangesAsync(ct);
    }

    public async Task<IReadOnlyList<ColumnStatistics>> GetPartitionColumnStatisticsAsync(
        string dbName, string tableName, IList<string> partitionValues,
        IEnumerable<string> columnNames, CancellationToken ct = default)
    {
        var partition = await RequirePartitionAsync(dbName, tableName, partitionValues, ct);
        var cols = columnNames.ToList();

        return await db.ColumnStatistics
            .Where(cs => cs.PartitionId == partition.Id && cols.Contains(cs.ColumnName))
            .ToListAsync(ct);
    }

    public async Task UpdatePartitionColumnStatisticsAsync(
        string dbName, string tableName, IList<string> partitionValues,
        IEnumerable<ColumnStatistics> stats, CancellationToken ct = default)
    {
        var partition = await RequirePartitionAsync(dbName, tableName, partitionValues, ct);

        foreach (var stat in stats)
        {
            var existing = await db.ColumnStatistics
                .FirstOrDefaultAsync(cs => cs.PartitionId == partition.Id && cs.ColumnName == stat.ColumnName, ct);

            if (existing is not null)
            {
                CopyStats(stat, existing);
                existing.LastAnalyzed = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            }
            else
            {
                stat.PartitionId = partition.Id;
                stat.TableId = null;
                stat.LastAnalyzed = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                db.ColumnStatistics.Add(stat);
            }
        }

        await db.SaveChangesAsync(ct);
    }

    public async Task DeletePartitionColumnStatisticsAsync(
        string dbName, string tableName, IList<string> partitionValues,
        string? columnName, CancellationToken ct = default)
    {
        var partition = await RequirePartitionAsync(dbName, tableName, partitionValues, ct);

        var q = db.ColumnStatistics.Where(cs => cs.PartitionId == partition.Id);
        if (columnName is not null)
            q = q.Where(cs => cs.ColumnName == columnName);

        db.ColumnStatistics.RemoveRange(await q.ToListAsync(ct));
        await db.SaveChangesAsync(ct);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<HiveTable> RequireTableAsync(string dbName, string tableName, CancellationToken ct) =>
        await db.Tables
            .Include(t => t.Database)
            .FirstOrDefaultAsync(t =>
                t.Database.Name == dbName.ToLowerInvariant() &&
                t.Name == tableName.ToLowerInvariant(), ct)
        ?? throw new NoSuchObjectException($"Table '{dbName}.{tableName}' does not exist.");

    private async Task<HivePartition> RequirePartitionAsync(string dbName, string tableName, IList<string> values, CancellationToken ct)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);
        var serialized = System.Text.Json.JsonSerializer.Serialize(values as IEnumerable<string> ?? values.ToList());
        return await db.Partitions.FirstOrDefaultAsync(p => p.TableId == table.Id && p.ValuesJson == serialized, ct)
            ?? throw new NoSuchObjectException($"Partition not found in '{dbName}.{tableName}'.");
    }

    private static void CopyStats(ColumnStatistics src, ColumnStatistics dst)
    {
        dst.ColumnType = src.ColumnType;
        dst.StatisticsType = src.StatisticsType;
        dst.NumNulls = src.NumNulls;
        dst.NumDistinctValues = src.NumDistinctValues;
        dst.NumTrues = src.NumTrues;
        dst.NumFalses = src.NumFalses;
        dst.LongLow = src.LongLow;
        dst.LongHigh = src.LongHigh;
        dst.DoubleLow = src.DoubleLow;
        dst.DoubleHigh = src.DoubleHigh;
        dst.MaxColLen = src.MaxColLen;
        dst.AvgColLen = src.AvgColLen;
        dst.DecimalLow = src.DecimalLow;
        dst.DecimalHigh = src.DecimalHigh;
        dst.DateLow = src.DateLow;
        dst.DateHigh = src.DateHigh;
        dst.BitVector = src.BitVector;
    }
}
