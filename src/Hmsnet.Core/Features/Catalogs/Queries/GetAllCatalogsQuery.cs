using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Catalogs.Queries;

public record GetAllCatalogsQuery : IRequest<IReadOnlyList<Catalog>>, ICachedQuery
{
    public string CacheKey => "catalog:all";
    public IReadOnlyCollection<string> Tags => [CacheTagsExtra.CatalogList];
    public TimeSpan Ttl => CacheTtl.Long;
}

public record GetAllCatalogNamesQuery : IRequest<IReadOnlyList<string>>, ICachedQuery
{
    public string CacheKey => "catalog:names";
    public IReadOnlyCollection<string> Tags => [CacheTagsExtra.CatalogList];
    public TimeSpan Ttl => CacheTtl.Long;
}

public record GetCatalogQuery(string Name) : IRequest<Catalog?>, ICachedQuery
{
    public string CacheKey => $"catalog:get:{Name.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTagsExtra.CatalogList, CacheTagsExtra.Catalog(Name)];
    public TimeSpan Ttl => CacheTtl.Medium;
}

public record CatalogExistsQuery(string Name) : IRequest<bool>, ICachedQuery
{
    public string CacheKey => $"catalog:exists:{Name.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTagsExtra.CatalogList, CacheTagsExtra.Catalog(Name)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
