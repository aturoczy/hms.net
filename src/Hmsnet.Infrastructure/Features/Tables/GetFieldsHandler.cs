using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class GetFieldsHandler(ITableService svc)
    : IRequestHandler<GetFieldsQuery, IReadOnlyList<HiveColumn>>
{
    public Task<IReadOnlyList<HiveColumn>> Handle(GetFieldsQuery request, CancellationToken ct) =>
        svc.GetFieldsAsync(request.DbName, request.TableName, ct);
}
