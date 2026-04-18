namespace Hmsnet.Infrastructure.Caching;

/// <summary>
/// Options bound from the <c>Redis</c> configuration section.
/// </summary>
public sealed class RedisCacheOptions
{
    /// <summary>When false the service registers a no-op cache and never opens a connection.</summary>
    public bool Enabled { get; set; }

    /// <summary>StackExchange.Redis connection string, e.g. "localhost:6379".</summary>
    public string ConnectionString { get; set; } = "localhost:6379";

    /// <summary>Prefix applied to every cache key and tag set — lets multiple environments share a Redis instance.</summary>
    public string InstanceName { get; set; } = "hmsnet:";

    /// <summary>Redis logical database number (0-15 by default).</summary>
    public int Database { get; set; } = 0;
}
