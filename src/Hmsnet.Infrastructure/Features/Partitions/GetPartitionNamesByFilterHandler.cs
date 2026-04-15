using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class GetPartitionNamesByFilterHandler(IPartitionService partSvc)
    : IRequestHandler<GetPartitionNamesByFilterQuery, IReadOnlyList<string>>
{
    public Task<IReadOnlyList<string>> Handle(GetPartitionNamesByFilterQuery request, CancellationToken ct) =>
        partSvc.GetPartitionNamesByFilterAsync(request.DbName, request.TableName, request.Filter, request.MaxParts, ct);
}
