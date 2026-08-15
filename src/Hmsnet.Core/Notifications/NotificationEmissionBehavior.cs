using System.Text.Json;
using Hmsnet.Core.Models;
using MediatR;
using Microsoft.Extensions.Logging;

namespace Hmsnet.Core.Notifications;

/// <summary>
/// MediatR pipeline behavior that appends a <see cref="NotificationEvent"/>
/// to the sink after any request implementing
/// <see cref="IEventEmittingCommand"/> completes successfully. A handler
/// that throws will not emit an event — stale readers are preferable to
/// phantom events describing a change that never landed.
/// </summary>
public sealed class NotificationEmissionBehavior<TRequest, TResponse>(
    INotificationEventSink sink,
    ILogger<NotificationEmissionBehavior<TRequest, TResponse>> logger)
    : IPipelineBehavior<TRequest, TResponse>
    where TRequest : notnull
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken)
    {
        var response = await next(cancellationToken);

        if (request is IEventEmittingCommand cmd)
        {
            try
            {
                var payload = cmd.EventPayload is null
                    ? "{}"
                    : JsonSerializer.Serialize(cmd.EventPayload, JsonOptions);

                await sink.AppendAsync(new NotificationEvent
                {
                    EventType = cmd.EventType,
                    CatalogName = cmd.CatalogName,
                    DbName = cmd.DbName,
                    TableName = cmd.TableName,
                    Message = payload,
                }, cancellationToken);
            }
            catch (Exception ex)
            {
                // Swallow: the write itself succeeded, and the event log is
                // best-effort. Losing a notification is preferable to
                // rolling back a committed catalog change.
                logger.LogWarning(ex,
                    "Failed to append notification event for {Command}", typeof(TRequest).Name);
            }
        }

        return response;
    }
}
