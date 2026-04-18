using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Partitions.Results;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionByNameQuery(string DbName, string TableName, string PartitionName)
    : IRequest<PartitionWithKeysResult?>, ICachedQuery
{
    public string CacheKey =>
        $"partition:byname:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}:{PartitionName}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Partitions(DbName, TableName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
