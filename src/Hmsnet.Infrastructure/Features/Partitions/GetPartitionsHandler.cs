using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class GetPartitionsHandler(IPartitionService partSvc, ITableService tableSvc)
    : IRequestHandler<GetPartitionsQuery, PartitionsWithKeysResult>
{
    public async Task<PartitionsWithKeysResult> Handle(GetPartitionsQuery request, CancellationToken ct)
    {
        var partitions = await partSvc.GetPartitionsAsync(request.DbName, request.TableName, request.MaxParts, ct);
        var schema = await tableSvc.GetSchemaAsync(request.DbName, request.TableName, ct);
        var keys = schema.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
        return new(partitions, keys);
    }
}
