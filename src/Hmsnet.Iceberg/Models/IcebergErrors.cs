using System.Text.Json.Serialization;

namespace Hmsnet.Iceberg.Models;

public record IcebergErrorResponse(
    [property: JsonPropertyName("error")] IcebergErrorModel Error);

public record IcebergErrorModel(
    [property: JsonPropertyName("message")] string Message,
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("code")] int Code);
