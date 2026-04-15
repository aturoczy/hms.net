using Hmsnet.Core.Exceptions;
using Hmsnet.Iceberg.Models;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Iceberg;

/// <summary>Shared helpers for Iceberg REST controllers.</summary>
public abstract class IcebergControllerBase : ControllerBase
{
    /// <summary>
    /// Decode a namespace path segment: URL-unescape, then split on \x1F (unit separator)
    /// for multi-level namespaces. Returns the decoded parts.
    /// </summary>
    protected static List<string> DecodeNamespace(string encoded) =>
        Uri.UnescapeDataString(encoded)
           .Split('\x1F', StringSplitOptions.None)
           .ToList();

    protected static string NamespaceToDatabaseName(List<string> parts) => parts[0].ToLowerInvariant();

    /// <summary>Returns an Iceberg-formatted 404 response.</summary>
    protected IActionResult IcebergNotFound(string message) =>
        NotFound(new IcebergErrorResponse(new IcebergErrorModel(message, "NoSuchNamespaceException", 404)));

    /// <summary>Returns an Iceberg-formatted 404 for tables.</summary>
    protected IActionResult IcebergTableNotFound(string message) =>
        NotFound(new IcebergErrorResponse(new IcebergErrorModel(message, "NoSuchTableException", 404)));

    /// <summary>Returns an Iceberg-formatted 409 conflict response.</summary>
    protected IActionResult IcebergConflict(string message) =>
        Conflict(new IcebergErrorResponse(new IcebergErrorModel(message, "AlreadyExistsException", 409)));

    /// <summary>Returns an Iceberg-formatted 400 response.</summary>
    protected IActionResult IcebergBadRequest(string message) =>
        BadRequest(new IcebergErrorResponse(new IcebergErrorModel(message, "BadRequestException", 400)));

    /// <summary>Returns an Iceberg-formatted 500 response.</summary>
    protected IActionResult IcebergServerError(string message) =>
        StatusCode(500, new IcebergErrorResponse(new IcebergErrorModel(message, "ServerException", 500)));

    /// <summary>Wraps an async operation, mapping MetastoreExceptions to Iceberg HTTP errors.</summary>
    protected async Task<IActionResult> HandleAsync(Func<Task<IActionResult>> action)
    {
        try
        {
            return await action();
        }
        catch (NoSuchObjectException ex)
        {
            return IcebergNotFound(ex.Message);
        }
        catch (AlreadyExistsException ex)
        {
            return IcebergConflict(ex.Message);
        }
        catch (Hmsnet.Core.Exceptions.InvalidOperationException ex)
        {
            return IcebergBadRequest(ex.Message);
        }
    }
}
