using System.Text.Json.Serialization;

namespace Hmsnet.Iceberg.Models;

public record IcebergTableIdentifier(
    [property: JsonPropertyName("namespace")] List<string> Namespace,
    [property: JsonPropertyName("name")] string Name);

public record ListTablesResponse(
    [property: JsonPropertyName("identifiers")] List<IcebergTableIdentifier> Identifiers);

public record IcebergCreateTableRequest(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("schema")] IcebergSchema Schema,
    [property: JsonPropertyName("location")] string? Location = null,
    [property: JsonPropertyName("partition-spec")] IcebergPartitionSpec? PartitionSpec = null,
    [property: JsonPropertyName("write-order")] IcebergSortOrder? WriteOrder = null,
    [property: JsonPropertyName("stage-create")] bool StageCreate = false,
    [property: JsonPropertyName("properties")] Dictionary<string, string>? Properties = null);

public record IcebergLoadTableResponse(
    [property: JsonPropertyName("metadata-location")] string MetadataLocation,
    [property: JsonPropertyName("metadata")] IcebergTableMetadataV2 Metadata,
    [property: JsonPropertyName("config")] Dictionary<string, string>? Config = null);

public record IcebergCommitTableRequest(
    [property: JsonPropertyName("requirements")] List<TableRequirement> Requirements,
    [property: JsonPropertyName("updates")] List<TableUpdate> Updates,
    [property: JsonPropertyName("identifier")] IcebergTableIdentifier? Identifier = null);

public record IcebergCommitTableResponse(
    [property: JsonPropertyName("metadata-location")] string MetadataLocation,
    [property: JsonPropertyName("metadata")] IcebergTableMetadataV2 Metadata);

public record RenameTableRequest(
    [property: JsonPropertyName("source")] IcebergTableIdentifier Source,
    [property: JsonPropertyName("destination")] IcebergTableIdentifier Destination);

public record RegisterTableRequest(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("metadata-location")] string MetadataLocation);
