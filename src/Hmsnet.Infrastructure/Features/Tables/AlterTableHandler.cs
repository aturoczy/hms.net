using Hmsnet.Core.Features.Tables.Commands;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class AlterTableHandler(ITableService svc) : IRequestHandler<AlterTableCommand, HiveTable>
{
    public Task<HiveTable> Handle(AlterTableCommand request, CancellationToken ct) =>
        svc.AlterTableAsync(request.DbName, request.TableName, request.Updated, ct);
}
