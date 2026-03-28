using Hmsnet.Core.DTOs;
using Hmsnet.Core.Models;

namespace Hmsnet.Core.Mapping;

public static class MetastoreMapper
{
    // ── Database ──────────────────────────────────────────────────────────────

    public static HiveDatabase ToModel(this DatabaseRequest dto) => new()
    {
        Name = dto.Name,
        Description = dto.Description,
        LocationUri = dto.LocationUri,
        OwnerName = dto.OwnerName,
        OwnerType = Enum.TryParse<PrincipalType>(dto.OwnerType, true, out var ot) ? ot : PrincipalType.User,
        Parameters = dto.Parameters ?? []
    };

    public static DatabaseResponse ToDto(this HiveDatabase m) => new(
        m.Name, m.Description, m.LocationUri,
        m.OwnerName, m.OwnerType.ToString(), m.Parameters, m.CreateTime);

    // ── Column ────────────────────────────────────────────────────────────────

    public static ColumnDto ToDto(this HiveColumn c) => new(
        c.Name, c.TypeName, c.Comment, c.OrdinalPosition, c.IsPartitionKey);

    public static HiveColumn ToModel(this ColumnDto dto, bool isPartitionKey = false) => new()
    {
        Name = dto.Name,
        TypeName = dto.TypeName,
        Comment = dto.Comment,
        OrdinalPosition = dto.OrdinalPosition,
        IsPartitionKey = isPartitionKey
    };

    // ── SerDeInfo ─────────────────────────────────────────────────────────────

    public static SerDeInfoDto ToDto(this SerDeInfo s) => new(
        s.Name, s.SerializationLib, s.Parameters);

    public static SerDeInfo ToModel(this SerDeInfoDto dto) => new()
    {
        Name = dto.Name,
        SerializationLib = dto.SerializationLib,
        Parameters = dto.Parameters ?? []
    };

    // ── StorageDescriptor ─────────────────────────────────────────────────────

    public static StorageDescriptorDto ToDto(this StorageDescriptor sd) => new(
        sd.Location, sd.InputFormat, sd.OutputFormat, sd.Compressed, sd.NumBuckets,
        sd.SerDeInfo.ToDto(),
        sd.BucketColumns,
        sd.SortColumns.Select(s => new SortOrderDto(s.Column, (int)s.Order)).ToList(),
        sd.Parameters,
        sd.StoredAsSubDirectories);

    public static StorageDescriptor ToModel(this StorageDescriptorDto dto) => new()
    {
        Location = dto.Location,
        InputFormat = dto.InputFormat,
        OutputFormat = dto.OutputFormat,
        Compressed = dto.Compressed,
        NumBuckets = dto.NumBuckets,
        BucketColumns = dto.BucketColumns ?? [],
        SortColumns = dto.SortColumns?.Select(s => new SortOrder { Column = s.Column, Order = (SortDirection)s.Order }).ToList() ?? [],
        Parameters = dto.Parameters ?? [],
        StoredAsSubDirectories = dto.StoredAsSubDirectories,
        SerDeInfo = dto.SerDeInfo.ToModel()
    };

    // ── Table ─────────────────────────────────────────────────────────────────

    public static TableResponse ToDto(this HiveTable t) => new(
        t.Name,
        t.Database?.Name ?? string.Empty,
        t.Owner,
        t.TableType.ToString(),
        t.StorageDescriptor.ToDto(),
        t.Columns.Where(c => !c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).Select(c => c.ToDto()).ToList(),
        t.PartitionKeys.OrderBy(c => c.OrdinalPosition).Select(c => c.ToDto()).ToList(),
        t.ViewOriginalText,
        t.ViewExpandedText,
        t.Parameters,
        t.Temporary,
        t.CreateTime,
        t.LastAccessTime,
        t.Retention);

    public static HiveTable ToModel(this TableRequest dto)
    {
        var dataCols = dto.Columns.Select((c, i) => (c with { OrdinalPosition = i }).ToModel()).ToList();
        var partCols = dto.PartitionKeys?.Select((c, i) => (c with { OrdinalPosition = i }).ToModel(true)).ToList() ?? [];
        return new HiveTable
        {
            Name = dto.Name,
            Owner = dto.Owner,
            TableType = Enum.TryParse<TableType>(dto.TableType, true, out var tt) ? tt : TableType.ManagedTable,
            StorageDescriptor = dto.StorageDescriptor.ToModel(),
            Columns = [.. dataCols, .. partCols],
            ViewOriginalText = dto.ViewOriginalText,
            ViewExpandedText = dto.ViewExpandedText,
            Parameters = dto.Parameters ?? [],
            Temporary = dto.Temporary
        };
    }

    // ── Partition ─────────────────────────────────────────────────────────────

    public static PartitionResponse ToDto(this HivePartition p, IList<HiveColumn> partitionKeys) => new(
        p.Values,
        BuildPartitionName(partitionKeys, p.Values),
        p.StorageDescriptor.ToDto(),
        p.Parameters,
        p.CreateTime,
        p.LastAccessTime);

    public static HivePartition ToModel(this PartitionRequest dto, StorageDescriptor? fallbackSd = null) => new()
    {
        Values = dto.Values,
        StorageDescriptor = dto.StorageDescriptor?.ToModel() ?? fallbackSd ?? new StorageDescriptor(),
        Parameters = dto.Parameters ?? []
    };

    // ── ColumnStatistics ──────────────────────────────────────────────────────

    public static ColumnStatisticsDto ToDto(this ColumnStatistics cs) => new(
        cs.ColumnName, cs.ColumnType, cs.StatisticsType.ToString(), cs.LastAnalyzed,
        cs.NumNulls, cs.NumDistinctValues,
        cs.NumTrues, cs.NumFalses,
        cs.LongLow, cs.LongHigh,
        cs.DoubleLow, cs.DoubleHigh,
        cs.MaxColLen, cs.AvgColLen,
        cs.DecimalLow, cs.DecimalHigh,
        cs.DateLow, cs.DateHigh,
        cs.BitVector);

    public static ColumnStatistics ToModel(this ColumnStatisticsDto dto) => new()
    {
        ColumnName = dto.ColumnName,
        ColumnType = dto.ColumnType,
        StatisticsType = Enum.TryParse<StatisticsType>(dto.StatisticsType, true, out var st) ? st : StatisticsType.String,
        LastAnalyzed = dto.LastAnalyzed,
        NumNulls = dto.NumNulls,
        NumDistinctValues = dto.NumDistinctValues,
        NumTrues = dto.NumTrues, NumFalses = dto.NumFalses,
        LongLow = dto.LongLow, LongHigh = dto.LongHigh,
        DoubleLow = dto.DoubleLow, DoubleHigh = dto.DoubleHigh,
        MaxColLen = dto.MaxColLen, AvgColLen = dto.AvgColLen,
        DecimalLow = dto.DecimalLow, DecimalHigh = dto.DecimalHigh,
        DateLow = dto.DateLow, DateHigh = dto.DateHigh,
        BitVector = dto.BitVector
    };

    // ── Helpers ───────────────────────────────────────────────────────────────

    public static string BuildPartitionName(IList<HiveColumn> keys, IList<string> values)
    {
        if (keys.Count != values.Count)
            return string.Join("/", values);
        return string.Join("/", keys.Zip(values, (k, v) => $"{k.Name}={v}"));
    }
}
