using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class AddPartitionsHandler(IPartitionService partSvc, ITableService tableSvc)
    : IRequestHandler<AddPartitionsCommand, PartitionsWithKeysResult>
{
    public async Task<PartitionsWithKeysResult> Handle(AddPartitionsCommand request, CancellationToken ct)
    {
        var partitions = await partSvc.AddPartitionsAsync(request.DbName, request.TableName, request.Partitions, ct);
        var schema = await tableSvc.GetSchemaAsync(request.DbName, request.TableName, ct);
        var keys = schema.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
        return new(partitions, keys);
    }
}
