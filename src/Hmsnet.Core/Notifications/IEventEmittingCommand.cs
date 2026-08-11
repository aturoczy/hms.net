namespace Hmsnet.Core.Notifications;

/// <summary>
/// Marker implemented by MediatR commands that should append a row to the
/// metastore notification event log after they succeed. Downstream consumers
/// (replication, audit, CDC) poll <c>GET /api/events?after={id}</c> to
/// receive the stream in order.
/// </summary>
public interface IEventEmittingCommand
{
    /// <summary>e.g. <c>CREATE_DATABASE</c>, <c>DROP_TABLE</c>, <c>ADD_PARTITION</c>.</summary>
    string EventType { get; }

    string? CatalogName => null;
    string? DbName { get; }
    string? TableName => null;

    /// <summary>
    /// JSON-serialisable payload. Return anything — the emitter will
    /// <c>JsonSerializer.Serialize</c> it. Return <c>null</c> for an empty
    /// event with just the identifying fields.
    /// </summary>
    object? EventPayload => null;
}
