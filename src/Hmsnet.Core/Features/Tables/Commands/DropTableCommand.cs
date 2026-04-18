using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record DropTableCommand(string DbName, string TableName, bool DeleteData)
    : IRequest, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(DbName),
        CacheTags.Table(DbName, TableName),
        CacheTags.Partitions(DbName, TableName),
        CacheTags.Stats(DbName, TableName),
        CacheTags.Iceberg(DbName, TableName),
    ];
}
