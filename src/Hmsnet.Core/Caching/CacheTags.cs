namespace Hmsnet.Core.Caching;

/// <summary>
/// Central registry of cache tag patterns. Kept in one file so the read side
/// (<see cref="ICachedQuery.CacheTags"/>) and the write side
/// (<see cref="IInvalidatingCommand.InvalidatesTags"/>) cannot drift apart.
/// </summary>
public static class CacheTags
{
    // ── Databases ─────────────────────────────────────────────────────────
    public const string DatabaseList = "db:list";
    public static string Database(string name) => $"db:{name.ToLowerInvariant()}";

    // ── Tables ────────────────────────────────────────────────────────────
    public static string TableList(string dbName) => $"tables:{dbName.ToLowerInvariant()}";
    public static string Table(string dbName, string tableName) =>
        $"table:{dbName.ToLowerInvariant()}:{tableName.ToLowerInvariant()}";

    // ── Partitions ────────────────────────────────────────────────────────
    public static string Partitions(string dbName, string tableName) =>
        $"partitions:{dbName.ToLowerInvariant()}:{tableName.ToLowerInvariant()}";

    // ── Column statistics ─────────────────────────────────────────────────
    public static string Stats(string dbName, string tableName) =>
        $"stats:{dbName.ToLowerInvariant()}:{tableName.ToLowerInvariant()}";

    // ── Iceberg ───────────────────────────────────────────────────────────
    public static string Iceberg(string dbName, string tableName) =>
        $"iceberg:{dbName.ToLowerInvariant()}:{tableName.ToLowerInvariant()}";
    public const string IcebergNamespaceList = "iceberg:ns:list";
}
