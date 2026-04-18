using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionNamesByFilterQuery(string DbName, string TableName, string Filter, int MaxParts = -1)
    : IRequest<IReadOnlyList<string>>, ICachedQuery
{
    public string CacheKey =>
        $"partitions:names:filter:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}:{Filter}:{MaxParts}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Partitions(DbName, TableName)];
    public TimeSpan Ttl => TimeSpan.FromMinutes(1);
}
