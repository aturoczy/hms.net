using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Partitions.Results;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionsQuery(string DbName, string TableName, int MaxParts = -1)
    : IRequest<PartitionsWithKeysResult>, ICachedQuery
{
    public string CacheKey =>
        $"partitions:all:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}:{MaxParts}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Partitions(DbName, TableName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
