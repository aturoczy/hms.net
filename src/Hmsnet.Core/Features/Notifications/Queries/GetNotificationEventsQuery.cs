using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Notifications.Queries;

/// <summary>
/// Reads the next batch of notification events after <paramref name="AfterId"/>
/// (exclusive), capped at <paramref name="MaxEvents"/> (default 100, max 1000).
/// Not cached — always hits the sink so consumers see fresh events.
/// </summary>
public record GetNotificationEventsQuery(long AfterId = 0, int MaxEvents = 100)
    : IRequest<IReadOnlyList<NotificationEvent>>;

public record GetCurrentNotificationEventIdQuery : IRequest<long>;
