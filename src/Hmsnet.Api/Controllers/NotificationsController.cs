using Hmsnet.Core.DTOs;
using Hmsnet.Core.Features.Notifications.Queries;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Api.Controllers;

/// <summary>
/// Read side of the metastore notification event log. Long-poll style: give
/// us an <c>after</c> event id and we return the next batch.
/// Corresponds to Hive's <c>get_next_notification</c> Thrift call.
/// </summary>
[ApiController]
[Route("api/events")]
public class NotificationsController(ISender sender) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<NotificationEventBatch>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetEvents(
        [FromQuery] long after = 0,
        [FromQuery] int max = 100,
        CancellationToken ct = default)
    {
        var events = await sender.Send(new GetNotificationEventsQuery(after, max), ct);
        var dtos = events.Select(e => new NotificationEventDto(
            e.Id, e.EventTime, e.EventType,
            e.CatalogName, e.DbName, e.TableName,
            e.MessageFormat, e.Message)).ToList();

        var lastId = dtos.Count > 0 ? dtos[^1].Id : after;
        return Ok(new NotificationEventBatch(dtos, lastId));
    }

    [HttpGet("current")]
    [ProducesResponseType<long>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetCurrentEventId(CancellationToken ct) =>
        Ok(await sender.Send(new GetCurrentNotificationEventIdQuery(), ct));
}
