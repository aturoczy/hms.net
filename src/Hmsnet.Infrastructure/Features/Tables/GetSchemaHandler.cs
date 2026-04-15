using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class GetSchemaHandler(ITableService svc)
    : IRequestHandler<GetSchemaQuery, IReadOnlyList<HiveColumn>>
{
    public Task<IReadOnlyList<HiveColumn>> Handle(GetSchemaQuery request, CancellationToken ct) =>
        svc.GetSchemaAsync(request.DbName, request.TableName, ct);
}
