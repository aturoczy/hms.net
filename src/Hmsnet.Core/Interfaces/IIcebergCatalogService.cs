using Hmsnet.Core.Models;

namespace Hmsnet.Core.Interfaces;

public interface IIcebergCatalogService
{
    // ── Namespace operations ──────────────────────────────────────────────────

    Task<IReadOnlyList<HiveDatabase>> ListNamespacesAsync(CancellationToken ct = default);
    Task<HiveDatabase> CreateNamespaceAsync(string name, Dictionary<string, string> properties, CancellationToken ct = default);
    Task<HiveDatabase?> GetNamespaceAsync(string name, CancellationToken ct = default);
    Task<(List<string> Updated, List<string> Removed)> UpdateNamespacePropertiesAsync(
        string name, List<string> removals, Dictionary<string, string> updates, CancellationToken ct = default);
    Task DropNamespaceAsync(string name, CancellationToken ct = default);

    // ── Table operations ──────────────────────────────────────────────────────

    Task<IReadOnlyList<HiveTable>> ListTablesAsync(string dbName, CancellationToken ct = default);
    Task<IcebergTableMetadata> CreateTableAsync(string dbName, HiveTable table, string metadataLocation, string metadataJson, CancellationToken ct = default);
    Task<IcebergTableMetadata?> LoadTableAsync(string dbName, string tableName, CancellationToken ct = default);
    Task<IcebergTableMetadata> CommitTableAsync(string dbName, string tableName, string newMetadataLocation, string newMetadataJson, CancellationToken ct = default);
    Task DropTableAsync(string dbName, string tableName, bool purge, CancellationToken ct = default);
    Task<IcebergTableMetadata> RegisterTableAsync(string dbName, string tableName, string metadataLocation, string metadataJson, CancellationToken ct = default);
    Task RenameTableAsync(string fromDb, string fromTable, string toDb, string toTable, CancellationToken ct = default);
}
