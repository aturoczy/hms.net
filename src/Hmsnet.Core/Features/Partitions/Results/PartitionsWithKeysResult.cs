using Hmsnet.Core.Models;

namespace Hmsnet.Core.Features.Partitions.Results;

public record PartitionsWithKeysResult(IReadOnlyList<HivePartition> Partitions, IList<HiveColumn> PartitionKeys);
