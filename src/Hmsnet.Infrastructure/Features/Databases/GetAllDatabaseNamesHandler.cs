using Hmsnet.Core.Features.Databases.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Databases;

public class GetAllDatabaseNamesHandler(IDatabaseService svc)
    : IRequestHandler<GetAllDatabaseNamesQuery, IReadOnlyList<string>>
{
    public Task<IReadOnlyList<string>> Handle(GetAllDatabaseNamesQuery request, CancellationToken ct) =>
        svc.GetAllDatabaseNamesAsync(ct);
}
