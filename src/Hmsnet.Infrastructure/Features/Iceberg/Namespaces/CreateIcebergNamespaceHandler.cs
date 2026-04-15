using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Namespaces;

public class CreateIcebergNamespaceHandler(IIcebergCatalogService svc)
    : IRequestHandler<CreateIcebergNamespaceCommand, HiveDatabase>
{
    public Task<HiveDatabase> Handle(CreateIcebergNamespaceCommand request, CancellationToken ct) =>
        svc.CreateNamespaceAsync(request.Name, request.Properties, ct);
}
