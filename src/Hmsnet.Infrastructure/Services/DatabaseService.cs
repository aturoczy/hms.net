using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using InvalidOperationException = Hmsnet.Core.Exceptions.InvalidOperationException;

namespace Hmsnet.Infrastructure.Services;

public class DatabaseService(MetastoreDbContext db) : IDatabaseService
{
    public async Task<IReadOnlyList<string>> GetAllDatabaseNamesAsync(CancellationToken ct = default) =>
        await db.Databases.Select(d => d.Name).OrderBy(n => n).ToListAsync(ct);

    public async Task<IReadOnlyList<HiveDatabase>> GetAllDatabasesAsync(CancellationToken ct = default) =>
        await db.Databases.OrderBy(d => d.Name).ToListAsync(ct);

    public async Task<HiveDatabase?> GetDatabaseAsync(string name, CancellationToken ct = default) =>
        await db.Databases.FirstOrDefaultAsync(d => d.Name == name.ToLowerInvariant(), ct);

    public async Task<bool> DatabaseExistsAsync(string name, CancellationToken ct = default) =>
        await db.Databases.AnyAsync(d => d.Name == name.ToLowerInvariant(), ct);

    public async Task<HiveDatabase> CreateDatabaseAsync(HiveDatabase database, CancellationToken ct = default)
    {
        database.Name = database.Name.ToLowerInvariant();

        if (await DatabaseExistsAsync(database.Name, ct))
            throw new AlreadyExistsException($"Database '{database.Name}' already exists.");

        if (string.IsNullOrWhiteSpace(database.LocationUri))
            database.LocationUri = $"hdfs:///user/hive/warehouse/{database.Name}.db";

        db.Databases.Add(database);
        await db.SaveChangesAsync(ct);
        return database;
    }

    public async Task<HiveDatabase> AlterDatabaseAsync(string name, HiveDatabase updated, CancellationToken ct = default)
    {
        var existing = await GetDatabaseAsync(name, ct)
            ?? throw new NoSuchObjectException($"Database '{name}' does not exist.");

        existing.Description = updated.Description;
        existing.LocationUri = updated.LocationUri;
        existing.OwnerName = updated.OwnerName;
        existing.OwnerType = updated.OwnerType;
        existing.Parameters = updated.Parameters;

        await db.SaveChangesAsync(ct);
        return existing;
    }

    public async Task DropDatabaseAsync(string name, bool cascade, CancellationToken ct = default)
    {
        var database = await db.Databases
            .Include(d => d.Tables)
            .FirstOrDefaultAsync(d => d.Name == name.ToLowerInvariant(), ct)
            ?? throw new NoSuchObjectException($"Database '{name}' does not exist.");

        if (!cascade && database.Tables.Count > 0)
            throw new InvalidOperationException(
                $"Database '{name}' is not empty. Use cascade=true to drop with all tables.");

        db.Databases.Remove(database);
        await db.SaveChangesAsync(ct);
    }
}
