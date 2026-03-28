using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;

namespace Hmsnet.Api.Thrift;

/// <summary>
/// Hosted service that listens on a TCP port and handles binary Thrift requests
/// following the Hive Metastore wire protocol (port 9083 by default).
/// </summary>
public class ThriftMetastoreServer : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ThriftServerOptions _options;
    private readonly ILogger<ThriftMetastoreServer> _logger;

    public ThriftMetastoreServer(
        IServiceScopeFactory scopeFactory,
        IOptions<ThriftServerOptions> options,
        ILogger<ThriftMetastoreServer> logger)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var listener = new TcpListener(IPAddress.Any, _options.Port);
        listener.Start();
        _logger.LogInformation("Thrift Metastore server listening on port {Port}", _options.Port);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(stoppingToken);
                _ = HandleClientAsync(client, stoppingToken);
            }
        }
        catch (OperationCanceledException) { /* normal shutdown */ }
        finally
        {
            listener.Stop();
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<ThriftHmsHandler>();
        var processor = new HiveMetastoreProcessor(handler);

        using (client)
        {
        using var stream = client.GetStream();
        var protocol = new ThriftBinaryProtocol(stream);

        try
        {
            while (client.Connected && !ct.IsCancellationRequested)
            {
                await processor.ProcessAsync(protocol, ct);
            }
        }
        catch (EndOfStreamException) { /* client disconnected */ }
        catch (IOException) { /* network error */ }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            _logger.LogError(ex, "Error handling Thrift client");
        }
        } // end using (client)
    }
}

public sealed class ThriftServerOptions
{
    public int Port { get; set; } = 9083;
}
