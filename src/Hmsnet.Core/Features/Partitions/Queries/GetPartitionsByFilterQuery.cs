using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Partitions.Results;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionsByFilterQuery(string DbName, string TableName, string Filter, int MaxParts = -1)
    : IRequest<PartitionsWithKeysResult>, ICachedQuery
{
    public string CacheKey =>
        $"partitions:filter:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}:{Filter}:{MaxParts}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Partitions(DbName, TableName)];
    // Very short TTL: filter cardinality is unbounded so entries churn fast.
    public TimeSpan Ttl => TimeSpan.FromMinutes(1);
}
