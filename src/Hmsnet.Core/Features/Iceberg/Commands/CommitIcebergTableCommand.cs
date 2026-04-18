using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record CommitIcebergTableCommand(
    string DbName,
    string TableName,
    string NewMetadataLocation,
    string NewMetadataJson)
    : IRequest<IcebergTableMetadata>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.Iceberg(DbName, TableName),
        CacheTags.Table(DbName, TableName),
        CacheTags.Partitions(DbName, TableName),
        CacheTags.Stats(DbName, TableName),
    ];
}
