using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class GetPartitionNamesHandler(IPartitionService partSvc)
    : IRequestHandler<GetPartitionNamesQuery, IReadOnlyList<string>>
{
    public Task<IReadOnlyList<string>> Handle(GetPartitionNamesQuery request, CancellationToken ct) =>
        partSvc.GetPartitionNamesAsync(request.DbName, request.TableName, request.MaxParts, ct);
}
