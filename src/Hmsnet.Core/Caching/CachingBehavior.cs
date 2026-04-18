using MediatR;
using Microsoft.Extensions.Logging;

namespace Hmsnet.Core.Caching;

/// <summary>
/// MediatR pipeline behavior that applies cache-aside semantics to any
/// request implementing <see cref="ICachedQuery"/>. Requests that do not
/// implement the marker pass through untouched.
/// </summary>
public sealed class CachingBehavior<TRequest, TResponse>(
    ICacheService cache,
    ILogger<CachingBehavior<TRequest, TResponse>> logger)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : notnull
{
    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken)
    {
        if (request is not ICachedQuery cached)
            return await next(cancellationToken);

        var key = cached.CacheKey;

        var (found, value) = await cache.TryGetAsync<TResponse>(key, cancellationToken);
        if (found)
        {
            logger.LogDebug("Cache hit for {Key}", key);
            return value!;
        }

        logger.LogDebug("Cache miss for {Key}", key);
        var response = await next(cancellationToken);

        if (response is not null)
            await cache.SetAsync(key, response, cached.Ttl, cached.Tags, cancellationToken);

        return response;
    }
}
