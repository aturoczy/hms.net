using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionCountQuery(string DbName, string TableName)
    : IRequest<int>, ICachedQuery
{
    public string CacheKey => $"partitions:count:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Partitions(DbName, TableName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
