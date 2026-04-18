namespace Hmsnet.Core.Caching;

/// <summary>
/// No-op <see cref="ICacheService"/> used when caching is disabled in
/// configuration and as a safe default in unit tests. Every read is a miss,
/// writes are discarded, and invalidations silently succeed.
/// </summary>
public sealed class NullCacheService : ICacheService
{
    public Task<(bool Found, T? Value)> TryGetAsync<T>(string key, CancellationToken ct = default) =>
        Task.FromResult<(bool, T?)>((false, default));

    public Task SetAsync<T>(
        string key,
        T value,
        TimeSpan ttl,
        IReadOnlyCollection<string>? tags = null,
        CancellationToken ct = default) => Task.CompletedTask;

    public Task RemoveAsync(string key, CancellationToken ct = default) => Task.CompletedTask;

    public Task InvalidateTagsAsync(
        IReadOnlyCollection<string> tags,
        CancellationToken ct = default) => Task.CompletedTask;
}
