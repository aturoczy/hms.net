using Hmsnet.Api.Thrift;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Scalar.AspNetCore;
using Hmsnet.Core.Caching;
using Hmsnet.Core.Interfaces;
using Hmsnet.Infrastructure.Caching;
using Hmsnet.Infrastructure.Data;
using Hmsnet.Infrastructure.Features.Databases;
using Hmsnet.Infrastructure.Services;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// ── EF Core ───────────────────────────────────────────────────────────────────
// Provider is picked from Database:Provider — supports postgresql, sqlserver
// and sqlite. See Hmsnet.Infrastructure/Data/MetastoreDbContextRegistration.
builder.Services.AddMetastoreDbContext(builder.Configuration);

// ── Services ──────────────────────────────────────────────────────────────────
builder.Services.AddScoped<IDatabaseService, DatabaseService>();
builder.Services.AddScoped<ITableService, TableService>();
builder.Services.AddScoped<IPartitionService, PartitionService>();
builder.Services.AddScoped<IColumnStatisticsService, ColumnStatisticsService>();
builder.Services.AddScoped<ThriftHmsHandler>();

// ── Distributed cache (Redis) ─────────────────────────────────────────────────
// Registers ICacheService as either RedisCacheService (when Redis:Enabled=true)
// or NullCacheService. The MediatR pipeline behaviors below read/write through
// whichever one is active, so flipping the flag requires no code change.
builder.Services.AddHmsnetCaching(builder.Configuration);

builder.Services.AddMediatR(cfg =>
{
    cfg.RegisterServicesFromAssembly(typeof(CreateDatabaseHandler).Assembly);
    // Ordering matters: caching runs outermost (short-circuits on hit), then
    // invalidation wraps the handler so tags are evicted only after a successful
    // SaveChanges.
    cfg.AddOpenBehavior(typeof(CachingBehavior<,>));
    cfg.AddOpenBehavior(typeof(InvalidationBehavior<,>));
});

// ── Thrift server ─────────────────────────────────────────────────────────────
builder.Services.Configure<ThriftServerOptions>(builder.Configuration.GetSection("Thrift"));
builder.Services.AddHostedService<ThriftMetastoreServer>();

// ── OpenTelemetry ─────────────────────────────────────────────────────────────
var otlpEndpoint = builder.Configuration["OpenTelemetry:Endpoint"] ?? "http://localhost:4317";

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(
        serviceName: "Hmsnet",
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
                // skip health checks and OpenAPI/Scalar noise
                !ctx.Request.Path.StartsWithSegments("/openapi") &&
                !ctx.Request.Path.StartsWithSegments("/scalar");
        })
        .AddHttpClientInstrumentation(opts => opts.RecordException = true)
        // EF Core has built-in activity sources since EF 8
        .AddSource("Microsoft.EntityFrameworkCore")
        .AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint))
        .AddConsoleExporter())
    .WithMetrics(metrics => metrics
        .SetResourceBuilder(resourceBuilder)
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddRuntimeInstrumentation()  // GC, thread pool, memory
        .AddMeter("Hmsnet") // custom app meter (reserved for future use)
        .AddOtlpExporter(o => o.Endpoint = new Uri(otlpEndpoint)));

// ── Web API ───────────────────────────────────────────────────────────────────
builder.Services.AddControllers();
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
    app.MapOpenApi();          // serves /openapi/v1.json
    app.MapScalarApiReference(); // serves /scalar/v1 (interactive UI)
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
