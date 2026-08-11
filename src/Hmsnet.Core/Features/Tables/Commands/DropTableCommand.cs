using Hmsnet.Core.Caching;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record DropTableCommand(string DbName, string TableName, bool DeleteData)
    : IRequest, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(DbName),
        CacheTags.Table(DbName, TableName),
        CacheTags.Partitions(DbName, TableName),
        CacheTags.Stats(DbName, TableName),
        CacheTags.Iceberg(DbName, TableName),
    ];

    string IEventEmittingCommand.EventType => "DROP_TABLE";
    string? IEventEmittingCommand.DbName => DbName;
    string? IEventEmittingCommand.TableName => TableName;
    object? IEventEmittingCommand.EventPayload => new { DeleteData };
}
