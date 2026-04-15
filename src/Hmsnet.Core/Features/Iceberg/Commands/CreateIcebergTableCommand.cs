using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record CreateIcebergTableCommand(
    string DbName,
    HiveTable Table,
    string MetadataLocation,
    string MetadataJson)
    : IRequest<IcebergTableMetadata>;
