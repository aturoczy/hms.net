using Hmsnet.Core.Caching;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record DropDatabaseCommand(string Name, bool Cascade)
    : IRequest, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.DatabaseList, CacheTags.Database(Name), CacheTags.TableList(Name)];

    public string EventType => "DROP_DATABASE";
    public string? DbName => Name;
    public object? EventPayload => new { Cascade };
}
