using Hmsnet.Core.Models;

namespace Hmsnet.Core.Interfaces;

public interface IDatabaseService
{
    Task<IReadOnlyList<string>> GetAllDatabaseNamesAsync(CancellationToken ct = default);
    Task<IReadOnlyList<HiveDatabase>> GetAllDatabasesAsync(CancellationToken ct = default);
    Task<HiveDatabase?> GetDatabaseAsync(string name, CancellationToken ct = default);
    Task<HiveDatabase> CreateDatabaseAsync(HiveDatabase database, CancellationToken ct = default);
    Task<HiveDatabase> AlterDatabaseAsync(string name, HiveDatabase updated, CancellationToken ct = default);
    Task DropDatabaseAsync(string name, bool cascade, CancellationToken ct = default);
    Task<bool> DatabaseExistsAsync(string name, CancellationToken ct = default);
}
