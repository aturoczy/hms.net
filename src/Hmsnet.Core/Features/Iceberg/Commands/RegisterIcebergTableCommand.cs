using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record RegisterIcebergTableCommand(
    string DbName,
    string TableName,
    string MetadataLocation,
    string MetadataJson)
    : IRequest<IcebergTableMetadata>;
