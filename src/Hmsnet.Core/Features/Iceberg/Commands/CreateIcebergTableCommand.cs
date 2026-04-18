using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record CreateIcebergTableCommand(
    string DbName,
    HiveTable Table,
    string MetadataLocation,
    string MetadataJson)
    : IRequest<IcebergTableMetadata>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(DbName),
        CacheTags.Table(DbName, Table.Name),
        CacheTags.Iceberg(DbName, Table.Name),
    ];
}
