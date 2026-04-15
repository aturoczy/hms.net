using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class DropPartitionHandler(IPartitionService partSvc) : IRequestHandler<DropPartitionCommand, bool>
{
    public Task<bool> Handle(DropPartitionCommand request, CancellationToken ct) =>
        partSvc.DropPartitionAsync(request.DbName, request.TableName, request.Values, request.DeleteData, ct);
}
