using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Models;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

public record AddPartitionCommand(string DbName, string TableName, HivePartition Partition)
    : IRequest<PartitionWithKeysResult>, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.Partitions(DbName, TableName), CacheTags.Stats(DbName, TableName)];

    string IEventEmittingCommand.EventType => "ADD_PARTITION";
    string? IEventEmittingCommand.DbName => DbName;
    string? IEventEmittingCommand.TableName => TableName;
    object? IEventEmittingCommand.EventPayload => new { Partition.Values };
}
