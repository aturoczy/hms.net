namespace Hmsnet.Core.DTOs;

public record SerDeInfoDto(
    string? Name,
    string SerializationLib,
    Dictionary<string, string>? Parameters
);
