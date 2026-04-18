using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

public record AddPartitionCommand(string DbName, string TableName, HivePartition Partition)
    : IRequest<PartitionWithKeysResult>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.Partitions(DbName, TableName), CacheTags.Stats(DbName, TableName)];
}
