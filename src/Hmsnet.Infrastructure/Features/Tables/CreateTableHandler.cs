using Hmsnet.Core.Features.Tables.Commands;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class CreateTableHandler(ITableService svc) : IRequestHandler<CreateTableCommand, HiveTable>
{
    public Task<HiveTable> Handle(CreateTableCommand request, CancellationToken ct) =>
        svc.CreateTableAsync(request.Table, ct);
}
