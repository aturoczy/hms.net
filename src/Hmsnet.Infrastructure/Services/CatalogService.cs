using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using InvalidOperationException = Hmsnet.Core.Exceptions.InvalidOperationException;

namespace Hmsnet.Infrastructure.Services;

public class CatalogService(MetastoreDbContext db) : ICatalogService
{
    public async Task<IReadOnlyList<Catalog>> GetAllCatalogsAsync(CancellationToken ct = default) =>
        await db.Catalogs.OrderBy(c => c.Name).ToListAsync(ct);

    public async Task<IReadOnlyList<string>> GetAllCatalogNamesAsync(CancellationToken ct = default) =>
        await db.Catalogs.Select(c => c.Name).OrderBy(n => n).ToListAsync(ct);

    public async Task<Catalog?> GetCatalogAsync(string name, CancellationToken ct = default) =>
        await db.Catalogs.FirstOrDefaultAsync(c => c.Name == name.ToLowerInvariant(), ct);

    public async Task<bool> CatalogExistsAsync(string name, CancellationToken ct = default) =>
        await db.Catalogs.AnyAsync(c => c.Name == name.ToLowerInvariant(), ct);

    public async Task<Catalog> CreateCatalogAsync(Catalog catalog, CancellationToken ct = default)
    {
        catalog.Name = catalog.Name.ToLowerInvariant();

        if (await CatalogExistsAsync(catalog.Name, ct))
            throw new AlreadyExistsException($"Catalog '{catalog.Name}' already exists.");

        db.Catalogs.Add(catalog);
        await db.SaveChangesAsync(ct);
        return catalog;
    }

    public async Task<Catalog> AlterCatalogAsync(string name, Catalog updated, CancellationToken ct = default)
    {
        var existing = await GetCatalogAsync(name, ct)
            ?? throw new NoSuchObjectException($"Catalog '{name}' does not exist.");

        existing.Description = updated.Description;
        existing.LocationUri = updated.LocationUri;
        existing.Properties = updated.Properties;

        await db.SaveChangesAsync(ct);
        return existing;
    }

    public async Task DropCatalogAsync(string name, CancellationToken ct = default)
    {
        var normalized = name.ToLowerInvariant();

        if (normalized == Catalog.DefaultName)
            throw new InvalidOperationException(
                $"The default catalog '{Catalog.DefaultName}' cannot be dropped.");

        var catalog = await db.Catalogs
            .FirstOrDefaultAsync(c => c.Name == normalized, ct)
            ?? throw new NoSuchObjectException($"Catalog '{name}' does not exist.");

        var hasDbs = await db.Databases.AnyAsync(d => d.CatalogName == normalized, ct);
        if (hasDbs)
            throw new InvalidOperationException(
                $"Catalog '{name}' is not empty. Drop or reparent its databases first.");

        db.Catalogs.Remove(catalog);
        await db.SaveChangesAsync(ct);
    }
}
