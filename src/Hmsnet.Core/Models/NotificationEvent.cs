namespace Hmsnet.Core.Models;

/// <summary>
/// One row in the metastore's notification event log — the .NET equivalent
/// of Hive's <c>DbNotificationListener</c> table. Downstream consumers
/// (replication, audit, CDC, cache warmers) poll <c>GET /api/events</c> with
/// an <c>after</c> cursor to receive new events in order.
/// </summary>
public class NotificationEvent
{
    /// <summary>Monotonically increasing event id — the poll cursor.</summary>
    public long Id { get; set; }

    /// <summary>UTC unix seconds when the event was persisted.</summary>
    public long EventTime { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    /// <summary>e.g. <c>CREATE_DATABASE</c>, <c>DROP_TABLE</c>, <c>ADD_PARTITION</c>.</summary>
    public string EventType { get; set; } = string.Empty;

    public string? CatalogName { get; set; }
    public string? DbName { get; set; }
    public string? TableName { get; set; }

    /// <summary>Serialization format of <see cref="Message"/> — typically <c>json</c>.</summary>
    public string MessageFormat { get; set; } = "json";

    /// <summary>JSON payload describing the change (fields depend on <see cref="EventType"/>).</summary>
    public string Message { get; set; } = "{}";
}
