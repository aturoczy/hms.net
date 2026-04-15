using System.Text.Json;
using System.Text.Json.Serialization;

namespace Hmsnet.Iceberg.Models;

// ── Schema ─────────────────────────────────────────────────────────────────

public record IcebergNestedField(
    [property: JsonPropertyName("id")] int Id,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("required")] bool Required,
    [property: JsonPropertyName("type")] JsonElement Type,
    [property: JsonPropertyName("doc")] string? Doc = null);

public record IcebergSchema(
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("schema-id")] int SchemaId,
    [property: JsonPropertyName("fields")] List<IcebergNestedField> Fields,
    [property: JsonPropertyName("identifier-field-ids")] List<int>? IdentifierFieldIds = null);

// ── Partition ──────────────────────────────────────────────────────────────

public record IcebergPartitionField(
    [property: JsonPropertyName("source-id")] int SourceId,
    [property: JsonPropertyName("field-id")] int FieldId,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("transform")] string Transform);

public record IcebergPartitionSpec(
    [property: JsonPropertyName("spec-id")] int SpecId,
    [property: JsonPropertyName("fields")] List<IcebergPartitionField> Fields);

// ── Sort Order ─────────────────────────────────────────────────────────────

public record IcebergSortField(
    [property: JsonPropertyName("transform")] string Transform,
    [property: JsonPropertyName("source-id")] int SourceId,
    [property: JsonPropertyName("direction")] string Direction,
    [property: JsonPropertyName("null-order")] string NullOrder);

public record IcebergSortOrder(
    [property: JsonPropertyName("order-id")] int OrderId,
    [property: JsonPropertyName("fields")] List<IcebergSortField> Fields);

// ── Snapshot ───────────────────────────────────────────────────────────────

public record IcebergSnapshot(
    [property: JsonPropertyName("snapshot-id")] long SnapshotId,
    [property: JsonPropertyName("sequence-number")] long SequenceNumber,
    [property: JsonPropertyName("timestamp-ms")] long TimestampMs,
    [property: JsonPropertyName("manifest-list")] string ManifestList,
    [property: JsonPropertyName("summary")] Dictionary<string, string> Summary,
    [property: JsonPropertyName("parent-snapshot-id")] long? ParentSnapshotId = null,
    [property: JsonPropertyName("schema-id")] int? SchemaId = null);

public record IcebergSnapshotRef(
    [property: JsonPropertyName("snapshot-id")] long SnapshotId,
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("min-snapshots-to-keep")] int? MinSnapshotsToKeep = null,
    [property: JsonPropertyName("max-snapshot-age-ms")] long? MaxSnapshotAgeMs = null,
    [property: JsonPropertyName("max-ref-age-ms")] long? MaxRefAgeMs = null);

// ── Metadata log ───────────────────────────────────────────────────────────

public record IcebergMetadataLogEntry(
    [property: JsonPropertyName("metadata-file")] string MetadataFile,
    [property: JsonPropertyName("timestamp-ms")] long TimestampMs);

public record IcebergSnapshotLogEntry(
    [property: JsonPropertyName("snapshot-id")] long SnapshotId,
    [property: JsonPropertyName("timestamp-ms")] long TimestampMs);

// ── Table Metadata V2 ──────────────────────────────────────────────────────

public class IcebergTableMetadataV2
{
    [JsonPropertyName("format-version")]
    public int FormatVersion { get; set; } = 2;

    [JsonPropertyName("table-uuid")]
    public string TableUuid { get; set; } = Guid.NewGuid().ToString();

    [JsonPropertyName("location")]
    public string Location { get; set; } = string.Empty;

    [JsonPropertyName("last-sequence-number")]
    public long LastSequenceNumber { get; set; } = 0;

    [JsonPropertyName("last-updated-ms")]
    public long LastUpdatedMs { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    [JsonPropertyName("last-column-id")]
    public int LastColumnId { get; set; } = 0;

    [JsonPropertyName("current-schema-id")]
    public int CurrentSchemaId { get; set; } = 0;

    [JsonPropertyName("schemas")]
    public List<IcebergSchema> Schemas { get; set; } = [];

    [JsonPropertyName("default-spec-id")]
    public int DefaultSpecId { get; set; } = 0;

    [JsonPropertyName("partition-specs")]
    public List<IcebergPartitionSpec> PartitionSpecs { get; set; } = [];

    [JsonPropertyName("last-partition-id")]
    public int LastPartitionId { get; set; } = 999;

    [JsonPropertyName("default-sort-order-id")]
    public int DefaultSortOrderId { get; set; } = 0;

    [JsonPropertyName("sort-orders")]
    public List<IcebergSortOrder> SortOrders { get; set; } = [];

    [JsonPropertyName("properties")]
    public Dictionary<string, string> Properties { get; set; } = [];

    [JsonPropertyName("current-snapshot-id")]
    public long? CurrentSnapshotId { get; set; } = null;

    [JsonPropertyName("refs")]
    public Dictionary<string, IcebergSnapshotRef>? Refs { get; set; } = null;

    [JsonPropertyName("snapshots")]
    public List<IcebergSnapshot> Snapshots { get; set; } = [];

    [JsonPropertyName("statistics")]
    public List<JsonElement>? Statistics { get; set; } = null;

    [JsonPropertyName("snapshot-log")]
    public List<IcebergSnapshotLogEntry> SnapshotLog { get; set; } = [];

    [JsonPropertyName("metadata-log")]
    public List<IcebergMetadataLogEntry> MetadataLog { get; set; } = [];
}
