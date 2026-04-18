using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record RegisterIcebergTableCommand(
    string DbName,
    string TableName,
    string MetadataLocation,
    string MetadataJson)
    : IRequest<IcebergTableMetadata>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(DbName),
        CacheTags.Table(DbName, TableName),
        CacheTags.Iceberg(DbName, TableName),
    ];
}
