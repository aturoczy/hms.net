using Hmsnet.Core.Caching;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

public record DropPartitionCommand(string DbName, string TableName, List<string> Values, bool DeleteData)
    : IRequest<bool>, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.Partitions(DbName, TableName), CacheTags.Stats(DbName, TableName)];

    string IEventEmittingCommand.EventType => "DROP_PARTITION";
    string? IEventEmittingCommand.DbName => DbName;
    string? IEventEmittingCommand.TableName => TableName;
    object? IEventEmittingCommand.EventPayload => new { Values };
}
