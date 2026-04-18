using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record CreateTableCommand(HiveTable Table)
    : IRequest<HiveTable>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(Table.Database?.Name ?? string.Empty),
        CacheTags.Table(Table.Database?.Name ?? string.Empty, Table.Name),
    ];
}
