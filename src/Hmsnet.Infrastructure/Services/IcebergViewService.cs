using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace Hmsnet.Infrastructure.Services;

public class IcebergViewService(MetastoreDbContext db) : IIcebergViewService
{
    private async Task<HiveDatabase> RequireDatabaseAsync(string dbName, CancellationToken ct) =>
        await db.Databases.FirstOrDefaultAsync(d => d.Name == dbName.ToLowerInvariant(), ct)
        ?? throw new NoSuchObjectException($"Database '{dbName}' does not exist.");

    public async Task<IReadOnlyList<IcebergView>> ListViewsAsync(string dbName, CancellationToken ct = default)
    {
        var database = await RequireDatabaseAsync(dbName, ct);
        return await db.IcebergViews
            .Where(v => v.DatabaseId == database.Id)
            .OrderBy(v => v.Name)
            .ToListAsync(ct);
    }

    public async Task<IcebergView?> LoadViewAsync(string dbName, string viewName, CancellationToken ct = default)
    {
        var database = await RequireDatabaseAsync(dbName, ct);
        return await db.IcebergViews
            .FirstOrDefaultAsync(v => v.DatabaseId == database.Id && v.Name == viewName.ToLowerInvariant(), ct);
    }

    public async Task<bool> ViewExistsAsync(string dbName, string viewName, CancellationToken ct = default) =>
        await LoadViewAsync(dbName, viewName, ct) is not null;

    public async Task<IcebergView> CreateViewAsync(
        string dbName, string viewName,
        string metadataLocation, string metadataJson, int currentVersionId,
        CancellationToken ct = default)
    {
        var database = await RequireDatabaseAsync(dbName, ct);
        var name = viewName.ToLowerInvariant();

        if (await db.IcebergViews.AnyAsync(v => v.DatabaseId == database.Id && v.Name == name, ct))
            throw new AlreadyExistsException($"View '{dbName}.{viewName}' already exists.");

        var view = new IcebergView
        {
            Name = name,
            DatabaseId = database.Id,
            MetadataLocation = metadataLocation,
            MetadataJson = metadataJson,
            CurrentVersionId = currentVersionId,
        };

        db.IcebergViews.Add(view);
        await db.SaveChangesAsync(ct);
        return view;
    }

    public async Task<IcebergView> ReplaceViewAsync(
        string dbName, string viewName,
        string newMetadataLocation, string newMetadataJson, int newVersionId,
        CancellationToken ct = default)
    {
        var view = await LoadViewAsync(dbName, viewName, ct)
            ?? throw new NoSuchObjectException($"View '{dbName}.{viewName}' does not exist.");

        view.MetadataLocation = newMetadataLocation;
        view.MetadataJson = newMetadataJson;
        view.CurrentVersionId = newVersionId;
        view.LastAlteredTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        await db.SaveChangesAsync(ct);
        return view;
    }

    public async Task DropViewAsync(string dbName, string viewName, CancellationToken ct = default)
    {
        var view = await LoadViewAsync(dbName, viewName, ct)
            ?? throw new NoSuchObjectException($"View '{dbName}.{viewName}' does not exist.");

        db.IcebergViews.Remove(view);
        await db.SaveChangesAsync(ct);
    }

    public async Task RenameViewAsync(
        string fromDb, string fromView,
        string toDb, string toView,
        CancellationToken ct = default)
    {
        var view = await LoadViewAsync(fromDb, fromView, ct)
            ?? throw new NoSuchObjectException($"View '{fromDb}.{fromView}' does not exist.");

        var targetDb = await RequireDatabaseAsync(toDb, ct);
        var targetName = toView.ToLowerInvariant();

        if (await db.IcebergViews.AnyAsync(v => v.DatabaseId == targetDb.Id && v.Name == targetName, ct))
            throw new AlreadyExistsException($"View '{toDb}.{toView}' already exists.");

        view.DatabaseId = targetDb.Id;
        view.Name = targetName;
        view.LastAlteredTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        await db.SaveChangesAsync(ct);
    }
}
