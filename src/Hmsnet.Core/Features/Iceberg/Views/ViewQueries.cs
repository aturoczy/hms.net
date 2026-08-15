using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Views;

public record ListIcebergViewsQuery(string DbName)
    : IRequest<IReadOnlyList<IcebergView>>, ICachedQuery
{
    public string CacheKey => $"iceberg:views:list:{DbName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags => [CacheTagsExtra.IcebergViewList(DbName)];
    public TimeSpan Ttl => CacheTtl.Medium;
}

public record LoadIcebergViewQuery(string DbName, string ViewName)
    : IRequest<IcebergView?>, ICachedQuery
{
    public string CacheKey =>
        $"iceberg:view:load:{DbName.ToLowerInvariant()}:{ViewName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTagsExtra.IcebergView(DbName, ViewName), CacheTagsExtra.IcebergViewList(DbName)];
    public TimeSpan Ttl => CacheTtl.Short;
}

public record IcebergViewExistsQuery(string DbName, string ViewName)
    : IRequest<bool>, ICachedQuery
{
    public string CacheKey =>
        $"iceberg:view:exists:{DbName.ToLowerInvariant()}:{ViewName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTagsExtra.IcebergView(DbName, ViewName), CacheTagsExtra.IcebergViewList(DbName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
