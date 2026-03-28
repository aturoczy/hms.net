using Hmsnet.Core.Models;

namespace Hmsnet.Core.Interfaces;

public interface ITableService
{
    Task<IReadOnlyList<string>> GetAllTableNamesAsync(string dbName, CancellationToken ct = default);
    Task<IReadOnlyList<string>> GetTableNamesLikeAsync(string dbName, string pattern, CancellationToken ct = default);
    Task<IReadOnlyList<HiveTable>> GetAllTablesAsync(string dbName, CancellationToken ct = default);
    Task<HiveTable?> GetTableAsync(string dbName, string tableName, CancellationToken ct = default);
    Task<IReadOnlyList<HiveTable>> GetTablesAsync(string dbName, IEnumerable<string> tableNames, CancellationToken ct = default);
    Task<HiveTable> CreateTableAsync(HiveTable table, CancellationToken ct = default);
    Task<HiveTable> AlterTableAsync(string dbName, string tableName, HiveTable updated, CancellationToken ct = default);
    Task DropTableAsync(string dbName, string tableName, bool deleteData, CancellationToken ct = default);
    Task<bool> TableExistsAsync(string dbName, string tableName, CancellationToken ct = default);

    /// <summary>Returns the data columns (non-partition) of a table.</summary>
    Task<IReadOnlyList<HiveColumn>> GetFieldsAsync(string dbName, string tableName, CancellationToken ct = default);

    /// <summary>Returns all columns including partition keys.</summary>
    Task<IReadOnlyList<HiveColumn>> GetSchemaAsync(string dbName, string tableName, CancellationToken ct = default);
}
