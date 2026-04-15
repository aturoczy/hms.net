using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace Hmsnet.Iceberg.Tests.Helpers;

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
