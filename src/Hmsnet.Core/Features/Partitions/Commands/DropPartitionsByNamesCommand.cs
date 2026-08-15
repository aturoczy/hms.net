using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

/// <summary>
/// Batched equivalent of <see cref="DropPartitionByNameCommand"/> — drops
/// several partitions in a single round-trip. Mirrors the
/// <c>drop_partitions_by_names</c> addition in Hive 4.2's IMetaStoreClient.
/// </summary>
public record DropPartitionsByNamesCommand(
    string DbName,
    string TableName,
    List<string> PartitionNames,
    bool DeleteData) : IRequest<int>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.Partitions(DbName, TableName), CacheTags.Stats(DbName, TableName)];
}
