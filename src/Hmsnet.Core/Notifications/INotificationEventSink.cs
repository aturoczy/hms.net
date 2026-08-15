using Hmsnet.Core.Models;

namespace Hmsnet.Core.Notifications;

/// <summary>
/// Persists notification events. Kept as an abstraction so hosts that don't
/// want the event log (e.g. embedded scenarios) can register a no-op sink
/// without touching handlers.
/// </summary>
public interface INotificationEventSink
{
    Task AppendAsync(NotificationEvent evt, CancellationToken ct = default);
    Task<IReadOnlyList<NotificationEvent>> ReadAsync(long afterId, int maxEvents, CancellationToken ct = default);
    Task<long> GetCurrentEventIdAsync(CancellationToken ct = default);
}
