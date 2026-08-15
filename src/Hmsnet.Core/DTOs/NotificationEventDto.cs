namespace Hmsnet.Core.DTOs;

public record NotificationEventDto(
    long Id,
    long EventTime,
    string EventType,
    string? CatalogName,
    string? DbName,
    string? TableName,
    string MessageFormat,
    string Message);

public record NotificationEventBatch(
    IReadOnlyList<NotificationEventDto> Events,
    long LastEventId);
