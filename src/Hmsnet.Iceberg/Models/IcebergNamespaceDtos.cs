using System.Text.Json.Serialization;

namespace Hmsnet.Iceberg.Models;

public record ListNamespacesResponse(
    [property: JsonPropertyName("namespaces")] List<List<string>> Namespaces);

public record CreateNamespaceRequest(
    [property: JsonPropertyName("namespace")] List<string> Namespace,
    [property: JsonPropertyName("properties")] Dictionary<string, string>? Properties = null);

public record CreateNamespaceResponse(
    [property: JsonPropertyName("namespace")] List<string> Namespace,
    [property: JsonPropertyName("properties")] Dictionary<string, string> Properties);

public record GetNamespaceResponse(
    [property: JsonPropertyName("namespace")] List<string> Namespace,
    [property: JsonPropertyName("properties")] Dictionary<string, string> Properties);

public record UpdateNamespacePropertiesRequest(
    [property: JsonPropertyName("removals")] List<string>? Removals = null,
    [property: JsonPropertyName("updates")] Dictionary<string, string>? Updates = null);

public record UpdateNamespacePropertiesResponse(
    [property: JsonPropertyName("updated")] List<string> Updated,
    [property: JsonPropertyName("removed")] List<string> Removed,
    [property: JsonPropertyName("missing")] List<string>? Missing = null);
