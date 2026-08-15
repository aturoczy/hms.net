using Hmsnet.Core.Models;

namespace Hmsnet.Core.Interfaces;

public interface ICatalogService
{
    Task<IReadOnlyList<Catalog>> GetAllCatalogsAsync(CancellationToken ct = default);
    Task<IReadOnlyList<string>> GetAllCatalogNamesAsync(CancellationToken ct = default);
    Task<Catalog?> GetCatalogAsync(string name, CancellationToken ct = default);
    Task<bool> CatalogExistsAsync(string name, CancellationToken ct = default);
    Task<Catalog> CreateCatalogAsync(Catalog catalog, CancellationToken ct = default);
    Task<Catalog> AlterCatalogAsync(string name, Catalog updated, CancellationToken ct = default);
    Task DropCatalogAsync(string name, CancellationToken ct = default);
}
