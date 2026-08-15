using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record CreateTableCommand(HiveTable Table)
    : IRequest<HiveTable>, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(Table.Database?.Name ?? string.Empty),
        CacheTags.Table(Table.Database?.Name ?? string.Empty, Table.Name),
    ];

    public string EventType => "CREATE_TABLE";
    public string? DbName => Table.Database?.Name;
    public string? TableName => Table.Name;
}
