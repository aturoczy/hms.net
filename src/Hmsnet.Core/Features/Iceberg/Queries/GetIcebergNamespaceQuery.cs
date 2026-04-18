using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Queries;

public record GetIcebergNamespaceQuery(string Name) : IRequest<HiveDatabase?>, ICachedQuery
{
    public string CacheKey => $"iceberg:ns:{Name.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTags.IcebergNamespaceList, CacheTags.Database(Name)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
