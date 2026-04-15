using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record UpdateIcebergNamespacePropertiesCommand(
    string Name,
    List<string> Removals,
    Dictionary<string, string> Updates)
    : IRequest<(List<string> Updated, List<string> Removed)>;
