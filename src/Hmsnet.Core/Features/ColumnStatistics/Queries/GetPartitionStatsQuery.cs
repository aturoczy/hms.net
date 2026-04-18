using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.ColumnStatistics.Queries;

public record GetPartitionStatsQuery(
    string DbName, string TableName,
    List<string> PartitionValues,
    List<string> Columns) : IRequest<IReadOnlyList<Hmsnet.Core.Models.ColumnStatistics>>, ICachedQuery
{
    public string CacheKey =>
        $"stats:partition:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}:{string.Join('/', PartitionValues)}:{string.Join(',', Columns.Select(c => c.ToLowerInvariant()).OrderBy(c => c))}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Stats(DbName, TableName)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
