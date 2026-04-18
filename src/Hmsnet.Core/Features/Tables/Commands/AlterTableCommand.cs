using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record AlterTableCommand(string DbName, string TableName, HiveTable Updated)
    : IRequest<HiveTable>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(DbName),
        CacheTags.Table(DbName, TableName),
        CacheTags.Partitions(DbName, TableName),
        CacheTags.Stats(DbName, TableName),
    ];
}
