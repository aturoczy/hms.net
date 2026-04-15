using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record CommitIcebergTableCommand(
    string DbName,
    string TableName,
    string NewMetadataLocation,
    string NewMetadataJson)
    : IRequest<IcebergTableMetadata>;
