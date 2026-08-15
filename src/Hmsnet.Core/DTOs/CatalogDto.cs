namespace Hmsnet.Core.DTOs;

public record CatalogRequest(
    string Name,
    string? Description,
    string LocationUri,
    Dictionary<string, string>? Properties = null);

public record CatalogResponse(
    string Name,
    string? Description,
    string LocationUri,
    Dictionary<string, string> Properties,
    long CreateTime);
