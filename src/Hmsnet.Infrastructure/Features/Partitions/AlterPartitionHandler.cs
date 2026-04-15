using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class AlterPartitionHandler(IPartitionService partSvc, ITableService tableSvc)
    : IRequestHandler<AlterPartitionCommand, PartitionWithKeysResult>
{
    public async Task<PartitionWithKeysResult> Handle(AlterPartitionCommand request, CancellationToken ct)
    {
        var partition = await partSvc.AlterPartitionAsync(request.DbName, request.TableName, request.Updated, ct);
        var schema = await tableSvc.GetSchemaAsync(request.DbName, request.TableName, ct);
        var keys = schema.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
        return new(partition, keys);
    }
}
