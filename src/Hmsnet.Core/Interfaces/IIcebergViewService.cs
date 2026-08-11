using Hmsnet.Core.Models;

namespace Hmsnet.Core.Interfaces;

/// <summary>
/// Iceberg VIEW spec — CRUD for portable SQL views stored in the catalog.
/// </summary>
public interface IIcebergViewService
{
    Task<IReadOnlyList<IcebergView>> ListViewsAsync(string dbName, CancellationToken ct = default);
    Task<IcebergView?> LoadViewAsync(string dbName, string viewName, CancellationToken ct = default);
    Task<bool> ViewExistsAsync(string dbName, string viewName, CancellationToken ct = default);
    Task<IcebergView> CreateViewAsync(string dbName, string viewName, string metadataLocation, string metadataJson, int currentVersionId, CancellationToken ct = default);
    Task<IcebergView> ReplaceViewAsync(string dbName, string viewName, string newMetadataLocation, string newMetadataJson, int newVersionId, CancellationToken ct = default);
    Task DropViewAsync(string dbName, string viewName, CancellationToken ct = default);
    Task RenameViewAsync(string fromDb, string fromView, string toDb, string toView, CancellationToken ct = default);
}
