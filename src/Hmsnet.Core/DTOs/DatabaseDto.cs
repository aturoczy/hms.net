namespace Hmsnet.Core.DTOs;

public record DatabaseRequest(
    string Name,
    string? Description,
    string LocationUri,
    string? OwnerName,
    string OwnerType = "User",
    Dictionary<string, string>? Parameters = null
);

public record DatabaseResponse(
    string Name,
    string? Description,
    string LocationUri,
    string? OwnerName,
    string OwnerType,
    Dictionary<string, string> Parameters,
    long CreateTime
);
