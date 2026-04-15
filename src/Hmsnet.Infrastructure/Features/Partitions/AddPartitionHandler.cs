using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Partitions;

public class AddPartitionHandler(IPartitionService partSvc, ITableService tableSvc)
    : IRequestHandler<AddPartitionCommand, PartitionWithKeysResult>
{
    public async Task<PartitionWithKeysResult> Handle(AddPartitionCommand request, CancellationToken ct)
    {
        var partition = await partSvc.AddPartitionAsync(request.DbName, request.TableName, request.Partition, ct);
        var keys = await GetPartitionKeysAsync(request.DbName, request.TableName, ct);
        return new(partition, keys);
    }

    private async Task<IList<Hmsnet.Core.Models.HiveColumn>> GetPartitionKeysAsync(
        string dbName, string tableName, CancellationToken ct)
    {
        var schema = await tableSvc.GetSchemaAsync(dbName, tableName, ct);
        return schema.Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();
    }
}
