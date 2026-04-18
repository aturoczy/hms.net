using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record DropIcebergTableCommand(string DbName, string TableName, bool Purge)
    : IRequest, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(DbName),
        CacheTags.Table(DbName, TableName),
        CacheTags.Iceberg(DbName, TableName),
        CacheTags.Partitions(DbName, TableName),
        CacheTags.Stats(DbName, TableName),
    ];
}
