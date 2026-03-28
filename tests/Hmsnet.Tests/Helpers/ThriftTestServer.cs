using System.Net;
using System.Net.Sockets;
using Hmsnet.Api.Thrift;
using Hmsnet.Core.Interfaces;
using Hmsnet.Infrastructure.Data;
using Hmsnet.Infrastructure.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Hmsnet.Tests.Helpers;

/// <summary>
/// Spins up a real ThriftMetastoreServer in-process on a free port, backed by
/// an EF Core InMemory database. One instance per test class.
/// </summary>
public sealed class ThriftTestServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    public int Port { get; }

    private ThriftTestServer(WebApplication app, int port) { _app = app; Port = port; }

    public static async Task<ThriftTestServer> StartAsync()
    {
        var port = GetFreePort();
        var app = BuildApp(port);
        await app.StartAsync();
        await WaitForPortAsync(port);
        return new ThriftTestServer(app, port);
    }

    private static int GetFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static WebApplication BuildApp(int port)
    {
        var builder = WebApplication.CreateBuilder([]);
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();

        builder.Services.AddDbContext<MetastoreDbContext>(opts =>
            opts.UseInMemoryDatabase(Guid.NewGuid().ToString()));

        builder.Services.AddScoped<IDatabaseService, DatabaseService>();
        builder.Services.AddScoped<ITableService, TableService>();
        builder.Services.AddScoped<IPartitionService, PartitionService>();
        builder.Services.AddScoped<IColumnStatisticsService, ColumnStatisticsService>();
        builder.Services.AddScoped<ThriftHmsHandler>();

        builder.Services.Configure<ThriftServerOptions>(opts => opts.Port = port);
        builder.Services.AddHostedService<ThriftMetastoreServer>();

        return builder.Build();
    }

    private static async Task WaitForPortAsync(int port, int timeoutMs = 5000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (DateTime.UtcNow < deadline)
        {
            try { using var c = new TcpClient(); await c.ConnectAsync(IPAddress.Loopback, port); return; }
            catch { await Task.Delay(50); }
        }
        throw new TimeoutException($"ThriftMetastoreServer not ready on port {port}");
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
