using Hmsnet.Core.Features.Notifications.Queries;
using Hmsnet.Core.Models;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Notifications;

public class GetNotificationEventsHandler(INotificationEventSink sink)
    : IRequestHandler<GetNotificationEventsQuery, IReadOnlyList<NotificationEvent>>
{
    public Task<IReadOnlyList<NotificationEvent>> Handle(
        GetNotificationEventsQuery request, CancellationToken ct) =>
        sink.ReadAsync(request.AfterId, request.MaxEvents, ct);
}

public class GetCurrentNotificationEventIdHandler(INotificationEventSink sink)
    : IRequestHandler<GetCurrentNotificationEventIdQuery, long>
{
    public Task<long> Handle(GetCurrentNotificationEventIdQuery request, CancellationToken ct) =>
        sink.GetCurrentEventIdAsync(ct);
}
