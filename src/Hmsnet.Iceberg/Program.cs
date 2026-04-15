using System.Text.Json.Serialization;
using Hmsnet.Core.Interfaces;
using Hmsnet.Infrastructure.Data;
using Hmsnet.Infrastructure.Features.Iceberg.Namespaces;
using Hmsnet.Infrastructure.Services;
using Microsoft.EntityFrameworkCore;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// ── EF Core ───────────────────────────────────────────────────────────────────
var provider = builder.Configuration["Database:Provider"] ?? "sqlite";
var connectionString = builder.Configuration.GetConnectionString("Metastore")
    ?? "Data Source=metastore.db";

builder.Services.AddDbContext<MetastoreDbContext>(opts =>
{
    _ = provider.ToLowerInvariant() switch
    {
        "postgresql" or "postgres" => opts.UseNpgsql(connectionString,
            npg => npg.MigrationsAssembly("Hmsnet.Infrastructure")),
        _ => opts.UseSqlite(connectionString,
            sq => sq.MigrationsAssembly("Hmsnet.Infrastructure"))
    };
});

// ── Services ──────────────────────────────────────────────────────────────────
builder.Services.AddScoped<IDatabaseService, DatabaseService>();
builder.Services.AddScoped<ITableService, TableService>();
builder.Services.AddScoped<IPartitionService, PartitionService>();
builder.Services.AddScoped<IColumnStatisticsService, ColumnStatisticsService>();
builder.Services.AddScoped<IIcebergCatalogService, IcebergCatalogService>();
builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(typeof(CreateIcebergNamespaceHandler).Assembly));

// ── Web API ───────────────────────────────────────────────────────────────────
builder.Services.AddControllers()
    .AddJsonOptions(opts =>
    {
        opts.JsonSerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
    });
builder.Services.AddOpenApi();

var app = builder.Build();

// ── Auto-migrate on startup ───────────────────────────────────────────────────
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<MetastoreDbContext>();
    await db.Database.MigrateAsync();
}

// ── Middleware ────────────────────────────────────────────────────────────────
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
    app.MapScalarApiReference(opts => opts.WithTitle("Hmsnet Iceberg REST Catalog"));
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
