using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record CreateIcebergNamespaceCommand(string Name, Dictionary<string, string> Properties)
    : IRequest<HiveDatabase>;
