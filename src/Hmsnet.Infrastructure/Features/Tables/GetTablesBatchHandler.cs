using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class GetTablesBatchHandler(ITableService svc)
    : IRequestHandler<GetTablesBatchQuery, IReadOnlyList<HiveTable>>
{
    public Task<IReadOnlyList<HiveTable>> Handle(GetTablesBatchQuery request, CancellationToken ct) =>
        svc.GetTablesAsync(request.DbName, request.TableNames, ct);
}
