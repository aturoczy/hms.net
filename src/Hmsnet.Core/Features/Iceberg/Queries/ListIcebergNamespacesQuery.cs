using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Queries;

public record ListIcebergNamespacesQuery : IRequest<IReadOnlyList<HiveDatabase>>, ICachedQuery
{
    public string CacheKey => "iceberg:ns:list";
    public IReadOnlyCollection<string> Tags => [CacheTags.IcebergNamespaceList, CacheTags.DatabaseList];
    public TimeSpan Ttl => CacheTtl.Long;
}
