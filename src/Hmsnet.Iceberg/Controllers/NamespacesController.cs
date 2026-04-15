using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Features.Iceberg.Queries;
using Hmsnet.Iceberg.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Iceberg.Controllers;

[ApiController]
[Route("v1/namespaces")]
public class NamespacesController(ISender sender) : IcebergControllerBase
{
    /// <summary>GET /v1/namespaces — list all namespaces.</summary>
    [HttpGet]
    public async Task<IActionResult> ListNamespaces(CancellationToken ct)
    {
        var dbs = await sender.Send(new ListIcebergNamespacesQuery(), ct);
        var namespaces = dbs.Select(d => new List<string> { d.Name }).ToList();
        return Ok(new ListNamespacesResponse(namespaces));
    }

    /// <summary>POST /v1/namespaces — create a namespace.</summary>
    [HttpPost]
    public async Task<IActionResult> CreateNamespace([FromBody] CreateNamespaceRequest request, CancellationToken ct)
    {
        if (request.Namespace is null || request.Namespace.Count == 0)
            return IcebergBadRequest("Namespace must have at least one level.");
        if (request.Namespace.Count > 1)
            return IcebergBadRequest("Only single-level namespaces are supported.");

        return await HandleAsync(async () =>
        {
            var name = request.Namespace[0];
            var db = await sender.Send(
                new CreateIcebergNamespaceCommand(name, request.Properties ?? []), ct);
            return StatusCode(200, new CreateNamespaceResponse(
                [db.Name],
                db.Parameters));
        });
    }

    /// <summary>GET /v1/namespaces/{namespace} — get a namespace.</summary>
    [HttpGet("{ns}")]
    public async Task<IActionResult> GetNamespace(string ns, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        var db = await sender.Send(new GetIcebergNamespaceQuery(NamespaceToDatabaseName(parts)), ct);
        if (db is null) return IcebergNotFound($"Namespace '{ns}' does not exist.");
        return Ok(new GetNamespaceResponse([db.Name], db.Parameters));
    }

    /// <summary>HEAD /v1/namespaces/{namespace} — check if namespace exists.</summary>
    [HttpHead("{ns}")]
    public async Task<IActionResult> NamespaceExists(string ns, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        var db = await sender.Send(new GetIcebergNamespaceQuery(NamespaceToDatabaseName(parts)), ct);
        return db is null ? IcebergNotFound($"Namespace '{ns}' does not exist.") : Ok();
    }

    /// <summary>DELETE /v1/namespaces/{namespace} — drop a namespace.</summary>
    [HttpDelete("{ns}")]
    public async Task<IActionResult> DropNamespace(string ns, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        return await HandleAsync(async () =>
        {
            await sender.Send(new DropIcebergNamespaceCommand(NamespaceToDatabaseName(parts)), ct);
            return NoContent();
        });
    }

    /// <summary>POST /v1/namespaces/{namespace}/properties — update namespace properties.</summary>
    [HttpPost("{ns}/properties")]
    public async Task<IActionResult> UpdateNamespaceProperties(
        string ns, [FromBody] UpdateNamespacePropertiesRequest request, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        return await HandleAsync(async () =>
        {
            var (updated, removed) = await sender.Send(
                new UpdateIcebergNamespacePropertiesCommand(
                    NamespaceToDatabaseName(parts),
                    request.Removals ?? [],
                    request.Updates ?? []), ct);
            return Ok(new UpdateNamespacePropertiesResponse(updated, removed));
        });
    }
}
