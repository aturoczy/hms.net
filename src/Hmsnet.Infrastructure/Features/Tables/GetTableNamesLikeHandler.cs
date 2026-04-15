using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Tables;

public class GetTableNamesLikeHandler(ITableService svc)
    : IRequestHandler<GetTableNamesLikeQuery, IReadOnlyList<string>>
{
    public Task<IReadOnlyList<string>> Handle(GetTableNamesLikeQuery request, CancellationToken ct) =>
        svc.GetTableNamesLikeAsync(request.DbName, request.Pattern, ct);
}
