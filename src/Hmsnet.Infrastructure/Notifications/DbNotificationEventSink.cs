using Hmsnet.Core.Models;
using Hmsnet.Core.Notifications;
using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace Hmsnet.Infrastructure.Notifications;

/// <summary>
/// EF Core-backed <see cref="INotificationEventSink"/> — writes to and reads
/// from the <c>NotificationEvents</c> table. Corresponds to Hive's
/// <c>DbNotificationListener</c>.
/// </summary>
public sealed class DbNotificationEventSink(MetastoreDbContext db) : INotificationEventSink
{
    public async Task AppendAsync(NotificationEvent evt, CancellationToken ct = default)
    {
        db.NotificationEvents.Add(evt);
        await db.SaveChangesAsync(ct);
    }

    public async Task<IReadOnlyList<NotificationEvent>> ReadAsync(
        long afterId, int maxEvents, CancellationToken ct = default)
    {
        var take = maxEvents <= 0 ? 100 : Math.Min(maxEvents, 1000);
        return await db.NotificationEvents
            .Where(e => e.Id > afterId)
            .OrderBy(e => e.Id)
            .Take(take)
            .ToListAsync(ct);
    }

    public async Task<long> GetCurrentEventIdAsync(CancellationToken ct = default)
    {
        if (!await db.NotificationEvents.AnyAsync(ct)) return 0;
        return await db.NotificationEvents.MaxAsync(e => e.Id, ct);
    }
}
