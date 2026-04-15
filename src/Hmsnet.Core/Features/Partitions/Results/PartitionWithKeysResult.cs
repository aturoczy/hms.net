using Hmsnet.Core.Models;

namespace Hmsnet.Core.Features.Partitions.Results;

public record PartitionWithKeysResult(HivePartition Partition, IList<HiveColumn> PartitionKeys);
