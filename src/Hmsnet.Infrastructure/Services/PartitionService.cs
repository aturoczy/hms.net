using System.Text.RegularExpressions;
using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using Hmsnet.Core.Mapping;
using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace Hmsnet.Infrastructure.Services;

public class PartitionService(MetastoreDbContext db) : IPartitionService
{
    private IQueryable<HivePartition> PartitionsWithDetails =>
        db.Partitions
            .Include(p => p.Table)
                .ThenInclude(t => t.Database)
            .Include(p => p.Table)
                .ThenInclude(t => t.Columns)
            .Include(p => p.StorageDescriptor)
                .ThenInclude(sd => sd.SerDeInfo);

    private async Task<HiveTable> RequireTableAsync(string dbName, string tableName, CancellationToken ct)
    {
        var table = await db.Tables
            .Include(t => t.Database)
            .Include(t => t.Columns)
            .Include(t => t.StorageDescriptor)
                .ThenInclude(sd => sd.SerDeInfo)
            .FirstOrDefaultAsync(t =>
                t.Database.Name == dbName.ToLowerInvariant() &&
                t.Name == tableName.ToLowerInvariant(), ct)
            ?? throw new NoSuchObjectException($"Table '{dbName}.{tableName}' does not exist.");
        return table;
    }

    public async Task<HivePartition> AddPartitionAsync(string dbName, string tableName, HivePartition partition, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);
        var partKeys = table.Columns.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();

        if (partition.Values.Count != partKeys.Count)
            throw new Core.Exceptions.InvalidOperationException(
                $"Expected {partKeys.Count} partition values but got {partition.Values.Count}.");

        if (await PartitionExistsAsync(table.Id, partition.Values, ct))
            throw new AlreadyExistsException(
                $"Partition {MetastoreMapper.BuildPartitionName(partKeys, partition.Values)} already exists.");

        partition.TableId = table.Id;

        if (string.IsNullOrWhiteSpace(partition.StorageDescriptor.Location))
            partition.StorageDescriptor.Location = BuildPartitionLocation(table.StorageDescriptor.Location, partKeys, partition.Values);

        db.Partitions.Add(partition);
        await db.SaveChangesAsync(ct);
        return partition;
    }

    public async Task<IReadOnlyList<HivePartition>> AddPartitionsAsync(string dbName, string tableName, IEnumerable<HivePartition> partitions, CancellationToken ct = default)
    {
        var results = new List<HivePartition>();
        foreach (var p in partitions)
            results.Add(await AddPartitionAsync(dbName, tableName, p, ct));
        return results;
    }

    public async Task<HivePartition?> GetPartitionAsync(string dbName, string tableName, IList<string> values, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);
        var serialized = System.Text.Json.JsonSerializer.Serialize(values as IEnumerable<string> ?? values.ToList());
        return await PartitionsWithDetails.FirstOrDefaultAsync(p =>
            p.TableId == table.Id && p.ValuesJson == serialized, ct);
    }

    public async Task<HivePartition?> GetPartitionByNameAsync(string dbName, string tableName, string partitionName, CancellationToken ct = default)
    {
        var values = ParsePartitionName(partitionName);
        return await GetPartitionAsync(dbName, tableName, values, ct);
    }

    public async Task<IReadOnlyList<HivePartition>> GetPartitionsAsync(string dbName, string tableName, int maxParts = -1, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);
        var q = PartitionsWithDetails.Where(p => p.TableId == table.Id).OrderBy(p => p.Id);
        if (maxParts > 0) q = (IOrderedQueryable<HivePartition>)q.Take(maxParts);
        return await q.ToListAsync(ct);
    }

    public async Task<IReadOnlyList<HivePartition>> GetPartitionsByFilterAsync(string dbName, string tableName, string filter, int maxParts = -1, CancellationToken ct = default)
    {
        // Basic in-memory filter for now (full predicate pushdown is a future enhancement)
        var all = await GetPartitionsAsync(dbName, tableName, -1, ct);
        var table = await RequireTableAsync(dbName, tableName, ct);
        var partKeys = table.Columns.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();

        var filtered = all.Where(p => EvaluateFilter(filter, partKeys, p.Values)).ToList();
        return maxParts > 0 ? filtered.Take(maxParts).ToList() : filtered;
    }

    public async Task<IReadOnlyList<HivePartition>> GetPartitionsByNamesAsync(string dbName, string tableName, IEnumerable<string> names, CancellationToken ct = default)
    {
        var result = new List<HivePartition>();
        foreach (var name in names)
        {
            var p = await GetPartitionByNameAsync(dbName, tableName, name, ct);
            if (p is not null) result.Add(p);
        }
        return result;
    }

    public async Task<IReadOnlyList<string>> GetPartitionNamesAsync(string dbName, string tableName, int maxParts = -1, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);
        var partKeys = table.Columns.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
        var partitions = await GetPartitionsAsync(dbName, tableName, maxParts, ct);
        return partitions.Select(p => MetastoreMapper.BuildPartitionName(partKeys, p.Values)).ToList();
    }

    public async Task<IReadOnlyList<string>> GetPartitionNamesByFilterAsync(string dbName, string tableName, string filter, int maxParts = -1, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);
        var partKeys = table.Columns.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
        var partitions = await GetPartitionsByFilterAsync(dbName, tableName, filter, maxParts, ct);
        return partitions.Select(p => MetastoreMapper.BuildPartitionName(partKeys, p.Values)).ToList();
    }

    public async Task<HivePartition> AlterPartitionAsync(string dbName, string tableName, HivePartition updated, CancellationToken ct = default)
    {
        var existing = await GetPartitionAsync(dbName, tableName, updated.Values, ct)
            ?? throw new NoSuchObjectException($"Partition not found.");

        existing.Parameters = updated.Parameters;
        existing.LastAccessTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        existing.StorageDescriptor.Location = updated.StorageDescriptor.Location;
        existing.StorageDescriptor.InputFormat = updated.StorageDescriptor.InputFormat;
        existing.StorageDescriptor.OutputFormat = updated.StorageDescriptor.OutputFormat;
        existing.StorageDescriptor.Compressed = updated.StorageDescriptor.Compressed;
        existing.StorageDescriptor.SerDeInfo.SerializationLib = updated.StorageDescriptor.SerDeInfo.SerializationLib;
        existing.StorageDescriptor.SerDeInfo.Parameters = updated.StorageDescriptor.SerDeInfo.Parameters;

        await db.SaveChangesAsync(ct);
        return existing;
    }

    public async Task AlterPartitionsAsync(string dbName, string tableName, IEnumerable<HivePartition> updated, CancellationToken ct = default)
    {
        foreach (var p in updated)
            await AlterPartitionAsync(dbName, tableName, p, ct);
    }

    public async Task<bool> DropPartitionAsync(string dbName, string tableName, IList<string> values, bool deleteData, CancellationToken ct = default)
    {
        var partition = await GetPartitionAsync(dbName, tableName, values, ct);
        if (partition is null) return false;
        db.Partitions.Remove(partition);
        await db.SaveChangesAsync(ct);
        return true;
    }

    public async Task<bool> DropPartitionByNameAsync(string dbName, string tableName, string partitionName, bool deleteData, CancellationToken ct = default)
    {
        var values = ParsePartitionName(partitionName);
        return await DropPartitionAsync(dbName, tableName, values, deleteData, ct);
    }

    public async Task<int> GetPartitionCountAsync(string dbName, string tableName, CancellationToken ct = default)
    {
        var table = await RequireTableAsync(dbName, tableName, ct);
        return await db.Partitions.CountAsync(p => p.TableId == table.Id, ct);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private async Task<bool> PartitionExistsAsync(int tableId, IList<string> values, CancellationToken ct)
    {
        var serialized = System.Text.Json.JsonSerializer.Serialize(values as IEnumerable<string> ?? values.ToList());
        return await db.Partitions.AnyAsync(p => p.TableId == tableId && p.ValuesJson == serialized, ct);
    }

    private static string BuildPartitionLocation(string tableLocation, IList<HiveColumn> partKeys, IList<string> values)
    {
        var segments = partKeys.Zip(values, (k, v) => $"{k.Name}={v}");
        return $"{tableLocation.TrimEnd('/')}/{string.Join("/", segments)}";
    }

    private static List<string> ParsePartitionName(string name)
    {
        // e.g. "year=2024/month=01" → ["2024","01"]
        return name.Split('/')
            .Select(seg => seg.Contains('=') ? seg[(seg.IndexOf('=') + 1)..] : seg)
            .ToList();
    }

    private static bool EvaluateFilter(string filter, IList<HiveColumn> partKeys, IList<string> values)
    {
        // Very basic filter evaluation: key=value comparisons joined by AND/OR
        // A full SQL expression parser is a future enhancement
        if (string.IsNullOrWhiteSpace(filter)) return true;

        var kvMap = partKeys.Zip(values, (k, v) => (k.Name, v))
            .ToDictionary(x => x.Name, x => x.v, StringComparer.OrdinalIgnoreCase);

        try
        {
            return EvaluateExpression(filter.Trim(), kvMap);
        }
        catch
        {
            return true; // if we can't parse, return all
        }
    }

    private static bool EvaluateExpression(string expr, Dictionary<string, string> kvMap)
    {
        // Handle OR at top level
        var orParts = SplitOnKeyword(expr, " or ");
        if (orParts.Length > 1)
            return orParts.Any(p => EvaluateExpression(p.Trim(), kvMap));

        // Handle AND
        var andParts = SplitOnKeyword(expr, " and ");
        if (andParts.Length > 1)
            return andParts.All(p => EvaluateExpression(p.Trim(), kvMap));

        // Simple comparison: key op value
        var m = Regex.Match(expr, @"^(\w+)\s*(=|!=|<>|>=|<=|>|<)\s*'?([^']*)'?$");
        if (!m.Success) return true;

        var key = m.Groups[1].Value;
        var op = m.Groups[2].Value;
        var val = m.Groups[3].Value;

        if (!kvMap.TryGetValue(key, out var actual)) return true;

        return op switch
        {
            "=" => actual == val,
            "!=" or "<>" => actual != val,
            ">" => string.Compare(actual, val, StringComparison.Ordinal) > 0,
            ">=" => string.Compare(actual, val, StringComparison.Ordinal) >= 0,
            "<" => string.Compare(actual, val, StringComparison.Ordinal) < 0,
            "<=" => string.Compare(actual, val, StringComparison.Ordinal) <= 0,
            _ => true
        };
    }

    private static string[] SplitOnKeyword(string expr, string keyword) =>
        expr.Split(new[] { keyword }, StringSplitOptions.None);
}
