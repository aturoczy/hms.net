using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record CreateDatabaseCommand(HiveDatabase Database)
    : IRequest<HiveDatabase>, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.DatabaseList, CacheTags.Database(Database.Name)];

    public string EventType => "CREATE_DATABASE";
    public string? DbName => Database.Name;
    public object? EventPayload => new { Database.Name, Database.LocationUri, Database.OwnerName };
}
