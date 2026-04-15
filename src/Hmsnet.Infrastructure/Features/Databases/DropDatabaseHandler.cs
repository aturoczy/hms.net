using Hmsnet.Core.Features.Databases.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Databases;

public class DropDatabaseHandler(IDatabaseService svc) : IRequestHandler<DropDatabaseCommand>
{
    public Task Handle(DropDatabaseCommand request, CancellationToken ct) =>
        svc.DropDatabaseAsync(request.Name, request.Cascade, ct);
}
