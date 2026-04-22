using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Hmsnet.Infrastructure.Data;

/// <summary>
/// Used by EF Core CLI tools (dotnet ef migrations add / database update).
///
/// Provider selection (first match wins):
///   1. First CLI arg after `--`, e.g. `dotnet ef ... -- sqlserver`
///   2. <c>DB_PROVIDER</c> env var
///   3. Default: <c>postgresql</c> (matches production appsettings)
///
/// Connection string comes from <c>DB_CONNECTIONSTRING</c>; each provider has
/// a sensible localhost default so `dotnet ef migrations add` works without
/// any env setup.
/// </summary>
public class MetastoreDbContextFactory : IDesignTimeDbContextFactory<MetastoreDbContext>
{
    public MetastoreDbContext CreateDbContext(string[] args)
    {
        var provider = (args.Length > 0 ? args[0] :
            Environment.GetEnvironmentVariable("DB_PROVIDER") ?? "postgresql")
            .ToLowerInvariant();

        var connectionString = Environment.GetEnvironmentVariable("DB_CONNECTIONSTRING")
            ?? DefaultConnectionStringFor(provider);

        var builder = new DbContextOptionsBuilder<MetastoreDbContext>();

        switch (provider)
        {
            case "postgresql":
            case "postgres":
                builder.UseNpgsql(connectionString,
                    npg => npg.MigrationsAssembly("Hmsnet.Infrastructure"));
                break;
            case "sqlserver":
            case "mssql":
                builder.UseSqlServer(connectionString,
                    sql => sql.MigrationsAssembly("Hmsnet.Infrastructure"));
                break;
            case "sqlite":
                builder.UseSqlite(connectionString,
                    sq => sq.MigrationsAssembly("Hmsnet.Infrastructure"));
                break;
            default:
                throw new InvalidOperationException(
                    $"Unknown provider '{provider}'. Expected: postgresql, sqlserver, sqlite.");
        }

        return new MetastoreDbContext(builder.Options);
    }

    private static string DefaultConnectionStringFor(string provider) => provider switch
    {
        "postgresql" or "postgres" =>
            "Host=localhost;Port=5432;Database=metastore;Username=hmsnet;Password=hmsnet_secret",
        "sqlserver" or "mssql" =>
            "Server=localhost,1433;Database=metastore;User Id=sa;Password=Hmsnet_secret1;TrustServerCertificate=True",
        "sqlite" => "Data Source=metastore.db",
        _ => throw new InvalidOperationException($"Unknown provider '{provider}'.")
    };
}
