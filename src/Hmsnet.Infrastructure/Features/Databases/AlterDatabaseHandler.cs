using Hmsnet.Core.Features.Databases.Commands;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Databases;

public class AlterDatabaseHandler(IDatabaseService svc) : IRequestHandler<AlterDatabaseCommand, HiveDatabase>
{
    public Task<HiveDatabase> Handle(AlterDatabaseCommand request, CancellationToken ct) =>
        svc.AlterDatabaseAsync(request.Name, request.Updated, ct);
}
