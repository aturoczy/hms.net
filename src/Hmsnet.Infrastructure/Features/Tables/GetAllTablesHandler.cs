using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class GetAllTablesHandler(ITableService svc)
    : IRequestHandler<GetAllTablesQuery, IReadOnlyList<HiveTable>>
{
    public Task<IReadOnlyList<HiveTable>> Handle(GetAllTablesQuery request, CancellationToken ct) =>
        svc.GetAllTablesAsync(request.DbName, ct);
}
