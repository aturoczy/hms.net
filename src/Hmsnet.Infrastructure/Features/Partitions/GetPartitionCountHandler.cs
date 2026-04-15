using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class GetPartitionCountHandler(IPartitionService partSvc) : IRequestHandler<GetPartitionCountQuery, int>
{
    public Task<int> Handle(GetPartitionCountQuery request, CancellationToken ct) =>
        partSvc.GetPartitionCountAsync(request.DbName, request.TableName, ct);
}
