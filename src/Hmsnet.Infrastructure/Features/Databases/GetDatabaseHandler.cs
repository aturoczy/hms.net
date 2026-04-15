using Hmsnet.Core.Features.Databases.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Databases;

public class GetDatabaseHandler(IDatabaseService svc) : IRequestHandler<GetDatabaseQuery, HiveDatabase?>
{
    public Task<HiveDatabase?> Handle(GetDatabaseQuery request, CancellationToken ct) =>
        svc.GetDatabaseAsync(request.Name, ct);
}
