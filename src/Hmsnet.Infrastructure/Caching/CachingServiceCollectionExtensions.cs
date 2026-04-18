using Hmsnet.Core.Caching;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Hmsnet.Infrastructure.Caching;

public static class CachingServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="ICacheService"/>. When <c>Redis:Enabled</c> is true
    /// the Redis-backed implementation is wired up; otherwise a no-op cache is
    /// used so the pipeline behaviors remain installable without Redis.
    /// </summary>
    public static IServiceCollection AddHmsnetCaching(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<RedisCacheOptions>(configuration.GetSection("Redis"));

        var options = configuration.GetSection("Redis").Get<RedisCacheOptions>() ?? new RedisCacheOptions();

        if (!options.Enabled)
        {
            services.AddSingleton<ICacheService, NullCacheService>();
            return services;
        }

        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("Redis");
            try
            {
                var config = ConfigurationOptions.Parse(options.ConnectionString);
                config.AbortOnConnectFail = false;   // keep app healthy if Redis is down at startup
                config.ConnectRetry = 3;
                return ConnectionMultiplexer.Connect(config);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Redis connection failed; falling back to no-op cache");
                throw;
            }
        });

        services.AddSingleton<ICacheService, RedisCacheService>();
        return services;
    }
}
