using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record DropIcebergNamespaceCommand(string Name) : IRequest;
