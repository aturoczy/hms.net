using Hmsnet.Core.Features.Databases.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Databases;

public class GetAllDatabasesHandler(IDatabaseService svc)
    : IRequestHandler<GetAllDatabasesQuery, IReadOnlyList<HiveDatabase>>
{
    public Task<IReadOnlyList<HiveDatabase>> Handle(GetAllDatabasesQuery request, CancellationToken ct) =>
        svc.GetAllDatabasesAsync(ct);
}
