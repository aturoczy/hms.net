using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class GetAllTableNamesHandler(ITableService svc)
    : IRequestHandler<GetAllTableNamesQuery, IReadOnlyList<string>>
{
    public Task<IReadOnlyList<string>> Handle(GetAllTableNamesQuery request, CancellationToken ct) =>
        svc.GetAllTableNamesAsync(request.DbName, ct);
}
