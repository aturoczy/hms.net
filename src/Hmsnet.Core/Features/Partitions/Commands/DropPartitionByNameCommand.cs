using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

public record DropPartitionByNameCommand(string DbName, string TableName, string PartitionName, bool DeleteData)
    : IRequest<bool>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.Partitions(DbName, TableName), CacheTags.Stats(DbName, TableName)];
}
