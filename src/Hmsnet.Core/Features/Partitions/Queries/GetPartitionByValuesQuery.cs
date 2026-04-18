using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Partitions.Results;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionByValuesQuery(string DbName, string TableName, List<string> Values)
    : IRequest<PartitionWithKeysResult?>, ICachedQuery
{
    public string CacheKey =>
        $"partition:byvalues:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}:{string.Join('/', Values)}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Partitions(DbName, TableName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
