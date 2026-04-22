using Hmsnet.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Hmsnet.Infrastructure.Data;

/// <summary>
/// Shared DbContext wiring used by both Hmsnet.Api and Hmsnet.Iceberg hosts.
/// Keeps the provider switch in a single place so adding or tweaking a
/// backend only needs one edit.
/// </summary>
public static class MetastoreDbContextRegistration
{
    /// <summary>
    /// Registers <see cref="MetastoreDbContext"/> using the provider named in
    /// <c>Database:Provider</c>. Valid values (case-insensitive):
    /// <list type="bullet">
    ///   <item><c>postgresql</c> / <c>postgres</c> — Npgsql</item>
    ///   <item><c>sqlserver</c> / <c>mssql</c> — Microsoft SQL Server</item>
    ///   <item><c>sqlite</c> — fallback / local dev (default)</item>
    /// </list>
    /// The connection string is read from <c>ConnectionStrings:Metastore</c>.
    /// </summary>
    public static IServiceCollection AddMetastoreDbContext(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var provider = (configuration["Database:Provider"] ?? "sqlite").ToLowerInvariant();
        var connectionString = configuration.GetConnectionString("Metastore")
            ?? "Data Source=metastore.db";

        services.AddDbContext<MetastoreDbContext>(opts =>
        {
            switch (provider)
            {
                case "postgresql":
                case "postgres":
                    opts.UseNpgsql(connectionString,
                        npg => npg.MigrationsAssembly("Hmsnet.Infrastructure"));
                    break;
                case "sqlserver":
                case "mssql":
                    opts.UseSqlServer(connectionString,
                        sql => sql.MigrationsAssembly("Hmsnet.Infrastructure"));
                    break;
                case "sqlite":
                    opts.UseSqlite(connectionString,
                        sq => sq.MigrationsAssembly("Hmsnet.Infrastructure"));
                    break;
                default:
                    throw new InvalidOperationException(
                        $"Unknown Database:Provider '{provider}'. " +
                        "Expected one of: postgresql, sqlserver, sqlite.");
            }
        });

        return services;
    }
}
