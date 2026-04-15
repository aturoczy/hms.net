using Hmsnet.Core.Features.Iceberg.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Namespaces;

public class GetIcebergNamespaceHandler(IIcebergCatalogService svc)
    : IRequestHandler<GetIcebergNamespaceQuery, HiveDatabase?>
{
    public Task<HiveDatabase?> Handle(GetIcebergNamespaceQuery request, CancellationToken ct) =>
        svc.GetNamespaceAsync(request.Name, ct);
}
