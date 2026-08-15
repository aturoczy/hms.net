using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class DropPartitionsByNamesHandler(IPartitionService svc)
    : IRequestHandler<DropPartitionsByNamesCommand, int>
{
    public Task<int> Handle(DropPartitionsByNamesCommand request, CancellationToken ct) =>
        svc.DropPartitionsByNamesAsync(
            request.DbName, request.TableName, request.PartitionNames, request.DeleteData, ct);
}
