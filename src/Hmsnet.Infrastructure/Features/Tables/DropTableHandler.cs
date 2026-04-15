using Hmsnet.Core.Features.Tables.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class DropTableHandler(ITableService svc) : IRequestHandler<DropTableCommand>
{
    public Task Handle(DropTableCommand request, CancellationToken ct) =>
        svc.DropTableAsync(request.DbName, request.TableName, request.DeleteData, ct);
}
