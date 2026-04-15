using System.Text.Json.Serialization;
using Hmsnet.Core.Interfaces;
using Hmsnet.Infrastructure.Data;
using Hmsnet.Infrastructure.Features.Iceberg.Namespaces;
using Hmsnet.Infrastructure.Services;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
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

// ── OpenTelemetry ─────────────────────────────────────────────────────────────
var otlpEndpoint = builder.Configuration["OpenTelemetry:Endpoint"] ?? "http://localhost:4317";

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(
        serviceName: "Hmsnet.Iceberg",
        serviceVersion: typeof(Program).Assembly.GetName().Version?.ToString() ?? "0.0.0")
    .AddAttributes([
        new("deployment.environment", builder.Environment.EnvironmentName),
        new("host.name", Environment.MachineName)
    ]);

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .SetResourceBuilder(resourceBuilder)
        .AddAspNetCoreInstrumentation(opts =>
        {
            opts.RecordException = true;
            opts.Filter = ctx =>
                !ctx.Request.Path.StartsWithSegments("/openapi") &&
                !ctx.Request.Path.StartsWithSegments("/scalar");
        })
        .AddHttpClientInstrumentation(opts => opts.RecordException = true)
        .AddSource("Microsoft.EntityFrameworkCore")
        .AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint))
        .AddConsoleExporter())
    .WithMetrics(metrics => metrics
        .SetResourceBuilder(resourceBuilder)
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddRuntimeInstrumentation()
        .AddMeter("Hmsnet.Iceberg")
        .AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint)));

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
    app.MapScalarApiReference(opts => opts
        .WithTitle("Hmsnet Iceberg REST Catalog")
        .WithDefaultHttpClient(ScalarTarget.CSharp, ScalarClient.HttpClient));
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
