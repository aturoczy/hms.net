namespace Hmsnet.Core.Caching;

// Catalog / Notification / Iceberg View tag extensions live here so the
// original CacheTags class stays focused on the pre-existing shapes.
public static partial class CacheTagsExtra
{
    public const string CatalogList = "catalog:list";
    public static string Catalog(string name) => $"catalog:{name.ToLowerInvariant()}";

    public const string NotificationEventStream = "events:stream";

    public static string IcebergView(string dbName, string viewName) =>
        $"iceberg:view:{dbName.ToLowerInvariant()}:{viewName.ToLowerInvariant()}";
    public static string IcebergViewList(string dbName) =>
        $"iceberg:views:{dbName.ToLowerInvariant()}";
}
