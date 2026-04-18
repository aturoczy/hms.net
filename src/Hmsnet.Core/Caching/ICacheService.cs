namespace Hmsnet.Core.Caching;

/// <summary>
/// Distributed cache abstraction used by the query/command pipeline.
/// Implementations back this with Redis (production) or a no-op (when
/// caching is disabled / in unit tests).
/// </summary>
public interface ICacheService
{
    /// <summary>
    /// Attempts to read a value from the cache.
    /// <c>Found=false</c> distinguishes a miss from a legitimate
    /// <c>default(T)</c> hit (important for <see cref="bool"/> and other value types).
    /// </summary>
    Task<(bool Found, T? Value)> TryGetAsync<T>(string key, CancellationToken ct = default);

    /// <summary>
    /// Writes a value to the cache with the given TTL. When <paramref name="tags"/>
    /// is provided the key is added to each tag set so it can later be evicted
    /// together with related entries via <see cref="InvalidateTagsAsync"/>.
    /// </summary>
    Task SetAsync<T>(
        string key,
        T value,
        TimeSpan ttl,
        IReadOnlyCollection<string>? tags = null,
        CancellationToken ct = default);

    /// <summary>Removes a single key.</summary>
    Task RemoveAsync(string key, CancellationToken ct = default);

    /// <summary>
    /// Evicts every key associated with any of the supplied tags, then drops the
    /// tag sets themselves. Safe to call with tags that do not exist.
    /// </summary>
    Task InvalidateTagsAsync(IReadOnlyCollection<string> tags, CancellationToken ct = default);
}
