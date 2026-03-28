using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Hmsnet.Infrastructure.Data;

/// <summary>
/// Used by EF Core CLI tools (dotnet ef migrations add / database update).
/// </summary>
public class MetastoreDbContextFactory : IDesignTimeDbContextFactory<MetastoreDbContext>
{
    public MetastoreDbContext CreateDbContext(string[] args)
    {
        // Prefer PostgreSQL when the env var or first arg is "postgresql"
        var provider = Environment.GetEnvironmentVariable("DB_PROVIDER")
            ?? (args.Length > 0 ? args[0] : "postgresql");

        var connectionString = Environment.GetEnvironmentVariable("DB_CONNECTIONSTRING")
            ?? "Host=localhost;Port=5432;Database=metastore;Username=hmsnet;Password=hmsnet_secret";

        var builder = new DbContextOptionsBuilder<MetastoreDbContext>();
        if (provider.Equals("postgresql", StringComparison.OrdinalIgnoreCase)
            || provider.Equals("postgres", StringComparison.OrdinalIgnoreCase))
        {
            builder.UseNpgsql(connectionString,
                npg => npg.MigrationsAssembly("Hmsnet.Infrastructure"));
        }
        else
        {
            builder.UseSqlite(connectionString,
                sq => sq.MigrationsAssembly("Hmsnet.Infrastructure"));
        }

        return new MetastoreDbContext(builder.Options);
    }
}
