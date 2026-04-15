using Hmsnet.Core.Features.Databases.Commands;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Databases;

public class CreateDatabaseHandler(IDatabaseService svc) : IRequestHandler<CreateDatabaseCommand, HiveDatabase>
{
    public Task<HiveDatabase> Handle(CreateDatabaseCommand request, CancellationToken ct) =>
        svc.CreateDatabaseAsync(request.Database, ct);
}
