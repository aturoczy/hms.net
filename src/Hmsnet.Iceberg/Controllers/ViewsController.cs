using System.Text.Json;
using Hmsnet.Core.Features.Iceberg.Views;
using Hmsnet.Iceberg.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Iceberg.Controllers;

/// <summary>
/// Implements the Iceberg REST Catalog VIEW spec endpoints.
///   GET    /v1/namespaces/{ns}/views                  — list
///   POST   /v1/namespaces/{ns}/views                  — create
///   GET    /v1/namespaces/{ns}/views/{view}           — load
///   HEAD   /v1/namespaces/{ns}/views/{view}           — exists
///   POST   /v1/namespaces/{ns}/views/{view}           — replace (submit new metadata)
///   DELETE /v1/namespaces/{ns}/views/{view}           — drop
///   POST   /v1/views/rename                           — rename
/// </summary>
[ApiController]
public class ViewsController(ISender sender) : IcebergControllerBase
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    [HttpGet("v1/namespaces/{ns}/views")]
    public async Task<IActionResult> ListViews(string ns, CancellationToken ct)
    {
        var dbName = NamespaceToDatabaseName(DecodeNamespace(ns));
        return await HandleAsync(async () =>
        {
            var views = await sender.Send(new ListIcebergViewsQuery(dbName), ct);
            var identifiers = views
                .Select(v => new IcebergViewIdentifier([dbName], v.Name))
                .ToList();
            return Ok(new ListViewsResponse(identifiers));
        });
    }

    [HttpPost("v1/namespaces/{ns}/views")]
    public async Task<IActionResult> CreateView(
        string ns, [FromBody] IcebergCreateViewRequest request, CancellationToken ct)
    {
        var dbName = NamespaceToDatabaseName(DecodeNamespace(ns));

        return await HandleAsync(async () =>
        {
            var location = request.Location ?? $"/{dbName}/views/{request.Name}";
            var metadata = new IcebergViewMetadata(
                ViewUuid: Guid.NewGuid().ToString(),
                FormatVersion: 1,
                Location: location,
                CurrentVersionId: request.ViewVersion.VersionId,
                Versions: [request.ViewVersion],
                Properties: request.Properties);
            var metaJson = JsonSerializer.Serialize(metadata, JsonOpts);
            var metaLocation = $"{location}/metadata/v{request.ViewVersion.VersionId}.metadata.json";

            var view = await sender.Send(new CreateIcebergViewCommand(
                dbName, request.Name, metaLocation, metaJson, request.ViewVersion.VersionId), ct);

            return Ok(new LoadViewResponse(view.MetadataLocation, metadata));
        });
    }

    [HttpGet("v1/namespaces/{ns}/views/{view}")]
    public async Task<IActionResult> LoadView(string ns, string view, CancellationToken ct)
    {
        var dbName = NamespaceToDatabaseName(DecodeNamespace(ns));
        var stored = await sender.Send(new LoadIcebergViewQuery(dbName, view), ct);
        if (stored is null)
            return IcebergViewNotFound($"View '{dbName}.{view}' does not exist.");

        var metadata = JsonSerializer.Deserialize<IcebergViewMetadata>(stored.MetadataJson, JsonOpts)!;
        return Ok(new LoadViewResponse(stored.MetadataLocation, metadata));
    }

    [HttpHead("v1/namespaces/{ns}/views/{view}")]
    public async Task<IActionResult> ViewExists(string ns, string view, CancellationToken ct)
    {
        var dbName = NamespaceToDatabaseName(DecodeNamespace(ns));
        var exists = await sender.Send(new IcebergViewExistsQuery(dbName, view), ct);
        return exists
            ? Ok()
            : IcebergViewNotFound($"View '{dbName}.{view}' does not exist.");
    }

    [HttpPost("v1/namespaces/{ns}/views/{view}")]
    public async Task<IActionResult> ReplaceView(
        string ns, string view,
        [FromBody] IcebergViewMetadata newMetadata,
        CancellationToken ct)
    {
        var dbName = NamespaceToDatabaseName(DecodeNamespace(ns));

        return await HandleAsync(async () =>
        {
            var metaJson = JsonSerializer.Serialize(newMetadata, JsonOpts);
            var metaLocation = $"{newMetadata.Location}/metadata/v{newMetadata.CurrentVersionId}.metadata.json";

            var updated = await sender.Send(new ReplaceIcebergViewCommand(
                dbName, view, metaLocation, metaJson, newMetadata.CurrentVersionId), ct);

            return Ok(new LoadViewResponse(updated.MetadataLocation, newMetadata));
        });
    }

    [HttpDelete("v1/namespaces/{ns}/views/{view}")]
    public async Task<IActionResult> DropView(string ns, string view, CancellationToken ct)
    {
        var dbName = NamespaceToDatabaseName(DecodeNamespace(ns));
        return await HandleAsync(async () =>
        {
            await sender.Send(new DropIcebergViewCommand(dbName, view), ct);
            return NoContent();
        });
    }

    [HttpPost("v1/views/rename")]
    public async Task<IActionResult> RenameView(
        [FromBody] RenameViewRequest request, CancellationToken ct)
    {
        if (request.Source.Namespace.Count == 0 || request.Destination.Namespace.Count == 0)
            return IcebergBadRequest("Both source and destination must include a namespace.");

        var fromDb = request.Source.Namespace[0].ToLowerInvariant();
        var toDb = request.Destination.Namespace[0].ToLowerInvariant();

        return await HandleAsync(async () =>
        {
            await sender.Send(new RenameIcebergViewCommand(
                fromDb, request.Source.Name, toDb, request.Destination.Name), ct);
            return NoContent();
        });
    }

    private IActionResult IcebergViewNotFound(string message) =>
        NotFound(new IcebergErrorResponse(new IcebergErrorModel(message, "NoSuchViewException", 404)));
}
