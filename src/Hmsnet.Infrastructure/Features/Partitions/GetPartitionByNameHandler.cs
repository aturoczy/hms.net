using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class GetPartitionByNameHandler(IPartitionService partSvc, ITableService tableSvc)
    : IRequestHandler<GetPartitionByNameQuery, PartitionWithKeysResult?>
{
    public async Task<PartitionWithKeysResult?> Handle(GetPartitionByNameQuery request, CancellationToken ct)
    {
        var partition = await partSvc.GetPartitionByNameAsync(
            request.DbName, request.TableName, request.PartitionName, ct);
        if (partition is null) return null;
        var schema = await tableSvc.GetSchemaAsync(request.DbName, request.TableName, ct);
        var keys = schema.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
        return new(partition, keys);
    }
}
