using Hmsnet.Core.DTOs;
using Hmsnet.Core.Models;
using Mapster;

namespace Hmsnet.Core.Mapping;

public static class MetastoreMapper
{
    static MetastoreMapper() =>
        TypeAdapterConfig.GlobalSettings.Scan(typeof(MappingConfig).Assembly);

    // ── Database ──────────────────────────────────────────────────────────────

    public static HiveDatabase ToModel(this DatabaseRequest dto) => dto.Adapt<HiveDatabase>();

    public static DatabaseResponse ToDto(this HiveDatabase m) => m.Adapt<DatabaseResponse>();

    // ── Column ────────────────────────────────────────────────────────────────

    public static ColumnDto ToDto(this HiveColumn c) => c.Adapt<ColumnDto>();

    public static HiveColumn ToModel(this ColumnDto dto, bool isPartitionKey = false)
    {
        var model = dto.Adapt<HiveColumn>();
        model.IsPartitionKey = isPartitionKey;
        return model;
    }

    // ── SerDeInfo ─────────────────────────────────────────────────────────────

    public static SerDeInfoDto ToDto(this SerDeInfo s) => s.Adapt<SerDeInfoDto>();

    public static SerDeInfo ToModel(this SerDeInfoDto dto) => dto.Adapt<SerDeInfo>();

    // ── StorageDescriptor ─────────────────────────────────────────────────────

    public static StorageDescriptorDto ToDto(this StorageDescriptor sd) => sd.Adapt<StorageDescriptorDto>();

    public static StorageDescriptor ToModel(this StorageDescriptorDto dto) => dto.Adapt<StorageDescriptor>();

    // ── Table ─────────────────────────────────────────────────────────────────

    public static TableResponse ToDto(this HiveTable t) => t.Adapt<TableResponse>();

    public static HiveTable ToModel(this TableRequest dto)
    {
        var dataCols = dto.Columns
            .Select((c, i) => { var m = c.Adapt<HiveColumn>(); m.OrdinalPosition = i; m.IsPartitionKey = false; return m; })
            .ToList();
        var partCols = dto.PartitionKeys?
            .Select((c, i) => { var m = c.Adapt<HiveColumn>(); m.OrdinalPosition = i; m.IsPartitionKey = true; return m; })
            .ToList() ?? [];

        return new HiveTable
        {
            Name = dto.Name,
            Owner = dto.Owner,
            TableType = Enum.TryParse<TableType>(dto.TableType, true, out var tt) ? tt : TableType.ManagedTable,
            StorageDescriptor = dto.StorageDescriptor.Adapt<StorageDescriptor>(),
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
        p.StorageDescriptor.Adapt<StorageDescriptorDto>(),
        p.Parameters,
        p.CreateTime,
        p.LastAccessTime);

    public static HivePartition ToModel(this PartitionRequest dto, StorageDescriptor? fallbackSd = null) => new()
    {
        Values = dto.Values,
        StorageDescriptor = dto.StorageDescriptor?.Adapt<StorageDescriptor>() ?? fallbackSd ?? new StorageDescriptor(),
        Parameters = dto.Parameters ?? []
    };

    // ── ColumnStatistics ──────────────────────────────────────────────────────

    public static ColumnStatisticsDto ToDto(this ColumnStatistics cs) => cs.Adapt<ColumnStatisticsDto>();

    public static ColumnStatistics ToModel(this ColumnStatisticsDto dto) => dto.Adapt<ColumnStatistics>();

    // ── Helpers ───────────────────────────────────────────────────────────────

    public static string BuildPartitionName(IList<HiveColumn> keys, IList<string> values)
    {
        if (keys.Count != values.Count)
            return string.Join("/", values);
        return string.Join("/", keys.Zip(values, (k, v) => $"{k.Name}={v}"));
    }
}
