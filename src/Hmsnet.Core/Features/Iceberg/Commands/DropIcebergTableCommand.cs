using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record DropIcebergTableCommand(string DbName, string TableName, bool Purge) : IRequest;
