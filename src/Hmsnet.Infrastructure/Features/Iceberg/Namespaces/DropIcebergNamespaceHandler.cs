using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Namespaces;

public class DropIcebergNamespaceHandler(IIcebergCatalogService svc)
    : IRequestHandler<DropIcebergNamespaceCommand>
{
    public Task Handle(DropIcebergNamespaceCommand request, CancellationToken ct) =>
        svc.DropNamespaceAsync(request.Name, ct);
}
