using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Queries;

public record LoadIcebergTableQuery(string DbName, string TableName) : IRequest<IcebergTableMetadata?>;
