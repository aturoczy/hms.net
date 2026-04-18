using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

public record AddPartitionsCommand(string DbName, string TableName, List<HivePartition> Partitions)
    : IRequest<PartitionsWithKeysResult>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.Partitions(DbName, TableName), CacheTags.Stats(DbName, TableName)];
}
