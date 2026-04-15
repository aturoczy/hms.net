using Hmsnet.Core.Features.Databases.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Databases;

public class DatabaseExistsHandler(IDatabaseService svc) : IRequestHandler<DatabaseExistsQuery, bool>
{
    public Task<bool> Handle(DatabaseExistsQuery request, CancellationToken ct) =>
        svc.DatabaseExistsAsync(request.Name, ct);
}
