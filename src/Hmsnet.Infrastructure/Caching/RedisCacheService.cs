using System.Text.Json;
using System.Text.Json.Serialization;
using Hmsnet.Core.Caching;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Hmsnet.Infrastructure.Caching;

/// <summary>
/// StackExchange.Redis-backed <see cref="ICacheService"/>. Uses plain
/// <c>STRING</c> keys for cached payloads plus one <c>SET</c> per tag holding
/// the keys filed under that tag. Invalidation unions the tag sets, deletes
/// every referenced key, then drops the tag sets themselves.
/// </summary>
public sealed class RedisCacheService(
    IConnectionMultiplexer mux,
    IOptions<RedisCacheOptions> options,
    ILogger<RedisCacheService> logger) : ICacheService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        ReferenceHandler = ReferenceHandler.IgnoreCycles,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly RedisCacheOptions _options = options.Value;

    private IDatabase Db => mux.GetDatabase(_options.Database);
    private string Key(string key) => _options.InstanceName + key;
    private string TagKey(string tag) => _options.InstanceName + "tag:" + tag;

    public async Task<(bool Found, T? Value)> TryGetAsync<T>(string key, CancellationToken ct = default)
    {
        try
        {
            var value = await Db.StringGetAsync(Key(key));
            if (value.IsNullOrEmpty) return (false, default);
            var deserialized = JsonSerializer.Deserialize<T>((string)value!, JsonOptions);
            return (true, deserialized);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Redis GET failed for {Key} — falling back to origin", key);
            return (false, default);
        }
    }

    public async Task SetAsync<T>(
        string key,
        T value,
        TimeSpan ttl,
        IReadOnlyCollection<string>? tags = null,
        CancellationToken ct = default)
    {
        try
        {
            var payload = JsonSerializer.Serialize(value, JsonOptions);
            var redisKey = Key(key);

            var batch = Db.CreateBatch();
            var setTask = batch.StringSetAsync(redisKey, payload, ttl);

            var tagTasks = new List<Task>();
            if (tags is { Count: > 0 })
            {
                foreach (var tag in tags)
                {
                    var tagKey = TagKey(tag);
                    tagTasks.Add(batch.SetAddAsync(tagKey, redisKey));
                    // Give the tag set a TTL slightly larger than the entry so it
                    // can't live forever if invalidation never fires.
                    tagTasks.Add(batch.KeyExpireAsync(tagKey, ttl + TimeSpan.FromMinutes(5)));
                }
            }

            batch.Execute();
            await setTask;
            if (tagTasks.Count > 0) await Task.WhenAll(tagTasks);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Redis SET failed for {Key} — continuing without cache", key);
        }
    }

    public async Task RemoveAsync(string key, CancellationToken ct = default)
    {
        try
        {
            await Db.KeyDeleteAsync(Key(key));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Redis DEL failed for {Key}", key);
        }
    }

    public async Task InvalidateTagsAsync(
        IReadOnlyCollection<string> tags,
        CancellationToken ct = default)
    {
        if (tags.Count == 0) return;

        try
        {
            var db = Db;
            foreach (var tag in tags)
            {
                var tagKey = TagKey(tag);
                var members = await db.SetMembersAsync(tagKey);
                if (members.Length > 0)
                {
                    var keys = members
                        .Where(m => !m.IsNullOrEmpty)
                        .Select(m => (RedisKey)m.ToString())
                        .ToArray();
                    await db.KeyDeleteAsync(keys);
                }
                await db.KeyDeleteAsync(tagKey);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Redis tag invalidation failed for {Tags}", string.Join(',', tags));
        }
    }
}
