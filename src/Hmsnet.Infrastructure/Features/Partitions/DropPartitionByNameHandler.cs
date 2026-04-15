using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class DropPartitionByNameHandler(IPartitionService partSvc)
    : IRequestHandler<DropPartitionByNameCommand, bool>
{
    public Task<bool> Handle(DropPartitionByNameCommand request, CancellationToken ct) =>
        partSvc.DropPartitionByNameAsync(request.DbName, request.TableName, request.PartitionName, request.DeleteData, ct);
}
