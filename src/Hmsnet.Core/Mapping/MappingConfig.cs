using Hmsnet.Core.DTOs;
using Hmsnet.Core.Models;
using Mapster;

namespace Hmsnet.Core.Mapping;

public class MappingConfig : IRegister
{
    public void Register(TypeAdapterConfig config)
    {
        // ── HiveDatabase ──────────────────────────────────────────────────────
        config.NewConfig<HiveDatabase, DatabaseResponse>()
            .Map(dest => dest.OwnerType, src => src.OwnerType.ToString());

        config.NewConfig<DatabaseRequest, HiveDatabase>()
            .Map(dest => dest.OwnerType, src => ParsePrincipalType(src.OwnerType))
            .Map(dest => dest.Parameters, src => src.Parameters ?? new Dictionary<string, string>());

        // ── SortOrder ─────────────────────────────────────────────────────────
        config.NewConfig<SortOrder, SortOrderDto>()
            .Map(dest => dest.Order, src => (int)src.Order);

        config.NewConfig<SortOrderDto, SortOrder>()
            .Map(dest => dest.Order, src => (SortDirection)src.Order);

        // ── SerDeInfo ─────────────────────────────────────────────────────────
        config.NewConfig<SerDeInfoDto, SerDeInfo>()
            .Map(dest => dest.Parameters, src => src.Parameters ?? new Dictionary<string, string>());

        // ── StorageDescriptor ─────────────────────────────────────────────────
        config.NewConfig<StorageDescriptorDto, StorageDescriptor>()
            .Map(dest => dest.BucketColumns, src => src.BucketColumns ?? new List<string>())
            .Map(dest => dest.SortColumns, src => src.SortColumns != null
                ? src.SortColumns.Adapt<List<SortOrder>>()
                : new List<SortOrder>())
            .Map(dest => dest.Parameters, src => src.Parameters ?? new Dictionary<string, string>())
            .Map(dest => dest.SkewedInfo, src => new SkewedInfo());

        // ── HiveTable ─────────────────────────────────────────────────────────
        config.NewConfig<HiveTable, TableResponse>()
            .Map(dest => dest.DatabaseName, src => src.Database != null ? src.Database.Name : string.Empty)
            .Map(dest => dest.TableType, src => src.TableType.ToString())
            .Map(dest => dest.Columns, src => MapDataColumns(src))
            .Map(dest => dest.PartitionKeys, src => MapPartitionColumns(src));

        // ── ColumnStatistics ──────────────────────────────────────────────────
        config.NewConfig<ColumnStatistics, ColumnStatisticsDto>()
            .Map(dest => dest.StatisticsType, src => src.StatisticsType.ToString());

        config.NewConfig<ColumnStatisticsDto, ColumnStatistics>()
            .Map(dest => dest.StatisticsType, src => ParseStatisticsType(src.StatisticsType));
    }

    private static List<ColumnDto> MapDataColumns(HiveTable src) =>
        src.Columns
            .Where(c => !c.IsPartitionKey)
            .OrderBy(c => c.OrdinalPosition)
            .Select(c => c.Adapt<ColumnDto>())
            .ToList();

    private static List<ColumnDto> MapPartitionColumns(HiveTable src) =>
        src.PartitionKeys
            .OrderBy(c => c.OrdinalPosition)
            .Select(c => c.Adapt<ColumnDto>())
            .ToList();

    private static PrincipalType ParsePrincipalType(string? s) =>
        Enum.TryParse<PrincipalType>(s, true, out var v) ? v : PrincipalType.User;

    private static StatisticsType ParseStatisticsType(string? s) =>
        Enum.TryParse<StatisticsType>(s, true, out var v) ? v : StatisticsType.String;
}
