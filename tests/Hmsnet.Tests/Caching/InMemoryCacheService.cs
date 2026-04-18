using System.Collections.Concurrent;
using Hmsnet.Core.Caching;

namespace Hmsnet.Tests.Caching;

/// <summary>
/// Test double that implements <see cref="ICacheService"/> against an
/// in-process dictionary. Mirrors the Redis implementation's semantics —
/// including tag sets — so behavior tests exercise the same invalidation
/// code paths without needing a live Redis instance.
/// </summary>
public sealed class InMemoryCacheService : ICacheService
{
    private readonly ConcurrentDictionary<string, object?> _store = new();
    private readonly ConcurrentDictionary<string, HashSet<string>> _tags = new();

    public int SetCount;
    public int InvalidationCount;

    public Task<(bool Found, T? Value)> TryGetAsync<T>(string key, CancellationToken ct = default)
    {
        if (_store.TryGetValue(key, out var value))
            return Task.FromResult<(bool, T?)>((true, (T?)value));
        return Task.FromResult<(bool, T?)>((false, default));
    }

    public Task SetAsync<T>(
        string key,
        T value,
        TimeSpan ttl,
        IReadOnlyCollection<string>? tags = null,
        CancellationToken ct = default)
    {
        _store[key] = value;
        Interlocked.Increment(ref SetCount);

        if (tags is not null)
        {
            foreach (var tag in tags)
            {
                var set = _tags.GetOrAdd(tag, _ => new HashSet<string>());
                lock (set) set.Add(key);
            }
        }

        return Task.CompletedTask;
    }

    public Task RemoveAsync(string key, CancellationToken ct = default)
    {
        _store.TryRemove(key, out _);
        return Task.CompletedTask;
    }

    public Task InvalidateTagsAsync(IReadOnlyCollection<string> tags, CancellationToken ct = default)
    {
        Interlocked.Increment(ref InvalidationCount);

        foreach (var tag in tags)
        {
            if (_tags.TryRemove(tag, out var set))
            {
                lock (set)
                {
                    foreach (var k in set) _store.TryRemove(k, out _);
                }
            }
        }
        return Task.CompletedTask;
    }

    public int Count => _store.Count;
    public bool Contains(string key) => _store.ContainsKey(key);
}
