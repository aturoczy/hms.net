namespace Hmsnet.Core.Caching;

/// <summary>
/// TTL defaults used across queries. Values tuned to the metastore workload:
/// schema objects change via DDL (invalidation is the primary driver), TTL
/// exists only as a safety net when an invalidation is missed.
/// </summary>
public static class CacheTtl
{
    /// <summary>Database lists, table catalogs — rarely change.</summary>
    public static readonly TimeSpan Long = TimeSpan.FromHours(1);

    /// <summary>Single-row lookups (tables, schemas, column stats).</summary>
    public static readonly TimeSpan Medium = TimeSpan.FromMinutes(15);

    /// <summary>Partition metadata — more volatile, especially under Iceberg.</summary>
    public static readonly TimeSpan Short = TimeSpan.FromMinutes(5);
}
