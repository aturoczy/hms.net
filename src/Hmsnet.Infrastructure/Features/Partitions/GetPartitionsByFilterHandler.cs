using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class GetPartitionsByFilterHandler(IPartitionService partSvc, ITableService tableSvc)
    : IRequestHandler<GetPartitionsByFilterQuery, PartitionsWithKeysResult>
{
    public async Task<PartitionsWithKeysResult> Handle(GetPartitionsByFilterQuery request, CancellationToken ct)
    {
        var partitions = await partSvc.GetPartitionsByFilterAsync(
            request.DbName, request.TableName, request.Filter, request.MaxParts, ct);
        var schema = await tableSvc.GetSchemaAsync(request.DbName, request.TableName, ct);
        var keys = schema.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
        return new(partitions, keys);
    }
}
