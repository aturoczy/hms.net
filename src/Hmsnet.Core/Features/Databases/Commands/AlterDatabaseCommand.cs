using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record AlterDatabaseCommand(string Name, HiveDatabase Updated)
    : IRequest<HiveDatabase>, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.DatabaseList, CacheTags.Database(Name)];

    public string EventType => "ALTER_DATABASE";
    public string? DbName => Name;
}
