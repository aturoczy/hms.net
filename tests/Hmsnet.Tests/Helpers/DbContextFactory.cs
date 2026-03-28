using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace Hmsnet.Tests.Helpers;

/// <summary>
/// Creates a fresh in-memory <see cref="MetastoreDbContext"/> for each test
/// using a unique database name so tests are fully isolated.
/// </summary>
public static class DbContextFactory
{
    public static MetastoreDbContext Create(string? dbName = null)
    {
        var options = new DbContextOptionsBuilder<MetastoreDbContext>()
            .UseInMemoryDatabase(dbName ?? Guid.NewGuid().ToString())
            .Options;
        return new MetastoreDbContext(options);
    }
}
