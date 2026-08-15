using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record AlterTableCommand(string DbName, string TableName, HiveTable Updated)
    : IRequest<HiveTable>, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(DbName),
        CacheTags.Table(DbName, TableName),
        CacheTags.Partitions(DbName, TableName),
        CacheTags.Stats(DbName, TableName),
    ];

    string IEventEmittingCommand.EventType => "ALTER_TABLE";
    string? IEventEmittingCommand.DbName => DbName;
    string? IEventEmittingCommand.TableName => TableName;
}
