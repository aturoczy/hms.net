using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionNamesQuery(string DbName, string TableName, int MaxParts = -1)
    : IRequest<IReadOnlyList<string>>, ICachedQuery
{
    public string CacheKey =>
        $"partitions:names:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}:{MaxParts}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Partitions(DbName, TableName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
