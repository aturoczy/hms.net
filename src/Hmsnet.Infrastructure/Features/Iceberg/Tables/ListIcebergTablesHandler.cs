using Hmsnet.Core.Features.Iceberg.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Tables;

public class ListIcebergTablesHandler(IIcebergCatalogService svc)
    : IRequestHandler<ListIcebergTablesQuery, IReadOnlyList<HiveTable>>
{
    public Task<IReadOnlyList<HiveTable>> Handle(ListIcebergTablesQuery request, CancellationToken ct) =>
        svc.ListTablesAsync(request.DbName, ct);
}
