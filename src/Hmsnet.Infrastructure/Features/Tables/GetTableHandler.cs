using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class GetTableHandler(ITableService svc) : IRequestHandler<GetTableQuery, HiveTable?>
{
    public Task<HiveTable?> Handle(GetTableQuery request, CancellationToken ct) =>
        svc.GetTableAsync(request.DbName, request.TableName, ct);
}
