using MediatR;
using Microsoft.Extensions.Logging;

namespace Hmsnet.Core.Caching;

/// <summary>
/// MediatR pipeline behavior that evicts cache tags after a command handler
/// completes successfully. A handler that throws leaves the cache alone —
/// stale reads are preferable to acting on partially-committed state.
/// </summary>
public sealed class InvalidationBehavior<TRequest, TResponse>(
    ICacheService cache,
    ILogger<InvalidationBehavior<TRequest, TResponse>> logger)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : notnull
{
    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken)
    {
        var response = await next(cancellationToken);

        if (request is IInvalidatingCommand cmd)
        {
            var tags = cmd.InvalidatesTags;
            if (tags.Count > 0)
            {
                logger.LogDebug(
                    "Invalidating cache tags {Tags} after {Command}",
                    tags, typeof(TRequest).Name);
                await cache.InvalidateTagsAsync(tags, cancellationToken);
            }
        }

        return response;
    }
}
