using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class TableExistsHandler(ITableService svc) : IRequestHandler<TableExistsQuery, bool>
{
    public Task<bool> Handle(TableExistsQuery request, CancellationToken ct) =>
        svc.TableExistsAsync(request.DbName, request.TableName, ct);
}
