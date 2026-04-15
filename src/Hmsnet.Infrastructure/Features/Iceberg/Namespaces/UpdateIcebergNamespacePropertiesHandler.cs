using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Namespaces;

public class UpdateIcebergNamespacePropertiesHandler(IIcebergCatalogService svc)
    : IRequestHandler<UpdateIcebergNamespacePropertiesCommand, (List<string> Updated, List<string> Removed)>
{
    public Task<(List<string> Updated, List<string> Removed)> Handle(
        UpdateIcebergNamespacePropertiesCommand request, CancellationToken ct) =>
        svc.UpdateNamespacePropertiesAsync(request.Name, request.Removals, request.Updates, ct);
}
