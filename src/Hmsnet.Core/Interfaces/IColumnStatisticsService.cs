using Hmsnet.Core.Models;

namespace Hmsnet.Core.Interfaces;

public interface IColumnStatisticsService
{
    Task<IReadOnlyList<ColumnStatistics>> GetTableColumnStatisticsAsync(string dbName, string tableName, IEnumerable<string> columnNames, CancellationToken ct = default);
    Task UpdateTableColumnStatisticsAsync(string dbName, string tableName, IEnumerable<ColumnStatistics> stats, CancellationToken ct = default);
    Task DeleteTableColumnStatisticsAsync(string dbName, string tableName, string? columnName, CancellationToken ct = default);

    Task<IReadOnlyList<ColumnStatistics>> GetPartitionColumnStatisticsAsync(string dbName, string tableName, IList<string> partitionValues, IEnumerable<string> columnNames, CancellationToken ct = default);
    Task UpdatePartitionColumnStatisticsAsync(string dbName, string tableName, IList<string> partitionValues, IEnumerable<ColumnStatistics> stats, CancellationToken ct = default);
    Task DeletePartitionColumnStatisticsAsync(string dbName, string tableName, IList<string> partitionValues, string? columnName, CancellationToken ct = default);
}
