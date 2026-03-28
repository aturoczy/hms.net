using Hmsnet.Core.Models;

namespace Hmsnet.Core.Interfaces;

public interface IPartitionService
{
    Task<HivePartition> AddPartitionAsync(string dbName, string tableName, HivePartition partition, CancellationToken ct = default);
    Task<IReadOnlyList<HivePartition>> AddPartitionsAsync(string dbName, string tableName, IEnumerable<HivePartition> partitions, CancellationToken ct = default);

    Task<HivePartition?> GetPartitionAsync(string dbName, string tableName, IList<string> values, CancellationToken ct = default);
    Task<HivePartition?> GetPartitionByNameAsync(string dbName, string tableName, string partitionName, CancellationToken ct = default);
    Task<IReadOnlyList<HivePartition>> GetPartitionsAsync(string dbName, string tableName, int maxParts = -1, CancellationToken ct = default);
    Task<IReadOnlyList<HivePartition>> GetPartitionsByFilterAsync(string dbName, string tableName, string filter, int maxParts = -1, CancellationToken ct = default);
    Task<IReadOnlyList<HivePartition>> GetPartitionsByNamesAsync(string dbName, string tableName, IEnumerable<string> names, CancellationToken ct = default);

    Task<IReadOnlyList<string>> GetPartitionNamesAsync(string dbName, string tableName, int maxParts = -1, CancellationToken ct = default);
    Task<IReadOnlyList<string>> GetPartitionNamesByFilterAsync(string dbName, string tableName, string filter, int maxParts = -1, CancellationToken ct = default);

    Task<HivePartition> AlterPartitionAsync(string dbName, string tableName, HivePartition updated, CancellationToken ct = default);
    Task AlterPartitionsAsync(string dbName, string tableName, IEnumerable<HivePartition> updated, CancellationToken ct = default);

    Task<bool> DropPartitionAsync(string dbName, string tableName, IList<string> values, bool deleteData, CancellationToken ct = default);
    Task<bool> DropPartitionByNameAsync(string dbName, string tableName, string partitionName, bool deleteData, CancellationToken ct = default);

    Task<int> GetPartitionCountAsync(string dbName, string tableName, CancellationToken ct = default);
}
