namespace Hmsnet.Core.Caching;

/// <summary>
/// Marker implemented by MediatR queries that should be served from the
/// distributed cache. The pipeline <c>CachingBehavior</c> picks these up
/// automatically and applies a cache-aside fetch.
/// </summary>
public interface ICachedQuery
{
    /// <summary>
    /// Deterministic cache key for this query instance. Include every
    /// parameter that would change the response (db name, table name, filter,
    /// max-parts, …) so cache entries never collide.
    /// </summary>
    string CacheKey { get; }

    /// <summary>
    /// Tags the key should be filed under. A single tag is usually enough;
    /// multiple tags let one response be invalidated from different write
    /// paths (e.g. a table lookup joined with its database).
    /// </summary>
    IReadOnlyCollection<string> Tags { get; }

    /// <summary>
    /// TTL to apply when the entry is written. Acts as a safety net — even
    /// without explicit invalidation, stale data cannot live longer than this.
    /// </summary>
    TimeSpan Ttl { get; }
}
