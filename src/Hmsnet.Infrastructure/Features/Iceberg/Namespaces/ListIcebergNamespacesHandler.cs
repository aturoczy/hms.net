using Hmsnet.Core.Features.Iceberg.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Namespaces;

public class ListIcebergNamespacesHandler(IIcebergCatalogService svc)
    : IRequestHandler<ListIcebergNamespacesQuery, IReadOnlyList<HiveDatabase>>
{
    public Task<IReadOnlyList<HiveDatabase>> Handle(ListIcebergNamespacesQuery request, CancellationToken ct) =>
        svc.ListNamespacesAsync(ct);
}
