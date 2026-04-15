using System.Text.Json;
using System.Text.Json.Serialization;

namespace Hmsnet.Iceberg.Models;

// ── TableRequirement ──────────────────────────────────────────────────────

[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(AssertTableDoesNotExist), "assert-create")]
[JsonDerivedType(typeof(AssertRefSnapshotId), "assert-ref-snapshot-id")]
[JsonDerivedType(typeof(AssertLastAssignedFieldId), "assert-last-assigned-field-id")]
[JsonDerivedType(typeof(AssertCurrentSchemaId), "assert-current-schema-id")]
[JsonDerivedType(typeof(AssertLastAssignedPartitionId), "assert-last-assigned-partition-id")]
[JsonDerivedType(typeof(AssertDefaultSpecId), "assert-default-spec-id")]
[JsonDerivedType(typeof(AssertDefaultSortOrderId), "assert-default-sort-order-id")]
[JsonDerivedType(typeof(AssertTableUuid), "assert-table-uuid")]
public abstract class TableRequirement { }

public class AssertTableDoesNotExist : TableRequirement { }

public class AssertTableUuid : TableRequirement
{
    [JsonPropertyName("uuid")]
    public string Uuid { get; set; } = string.Empty;
}

public class AssertRefSnapshotId : TableRequirement
{
    [JsonPropertyName("ref")]
    public string Ref { get; set; } = string.Empty;
    [JsonPropertyName("snapshot-id")]
    public long? SnapshotId { get; set; }
}

public class AssertLastAssignedFieldId : TableRequirement
{
    [JsonPropertyName("last-assigned-field-id")]
    public int LastAssignedFieldId { get; set; }
}

public class AssertCurrentSchemaId : TableRequirement
{
    [JsonPropertyName("current-schema-id")]
    public int CurrentSchemaId { get; set; }
}

public class AssertLastAssignedPartitionId : TableRequirement
{
    [JsonPropertyName("last-assigned-partition-id")]
    public int LastAssignedPartitionId { get; set; }
}

public class AssertDefaultSpecId : TableRequirement
{
    [JsonPropertyName("default-spec-id")]
    public int DefaultSpecId { get; set; }
}

public class AssertDefaultSortOrderId : TableRequirement
{
    [JsonPropertyName("default-sort-order-id")]
    public int DefaultSortOrderId { get; set; }
}

// ── TableUpdate ────────────────────────────────────────────────────────────

[JsonPolymorphic(TypeDiscriminatorPropertyName = "action")]
[JsonDerivedType(typeof(UpgradeFormatVersionUpdate), "upgrade-format-version")]
[JsonDerivedType(typeof(AddSchemaUpdate), "add-schema")]
[JsonDerivedType(typeof(SetCurrentSchemaIdUpdate), "set-current-schema-id")]
[JsonDerivedType(typeof(AddPartitionSpecUpdate), "add-spec")]
[JsonDerivedType(typeof(SetDefaultSpecIdUpdate), "set-default-spec")]
[JsonDerivedType(typeof(AddSortOrderUpdate), "add-sort-order")]
[JsonDerivedType(typeof(SetDefaultSortOrderIdUpdate), "set-default-sort-order")]
[JsonDerivedType(typeof(AddSnapshotUpdate), "add-snapshot")]
[JsonDerivedType(typeof(SetSnapshotRefUpdate), "set-snapshot-ref")]
[JsonDerivedType(typeof(RemoveSnapshotsUpdate), "remove-snapshots")]
[JsonDerivedType(typeof(RemoveSnapshotRefUpdate), "remove-snapshot-ref")]
[JsonDerivedType(typeof(SetLocationUpdate), "set-location")]
[JsonDerivedType(typeof(SetPropertiesUpdate), "set-properties")]
[JsonDerivedType(typeof(RemovePropertiesUpdate), "remove-properties")]
public abstract class TableUpdate { }

public class UpgradeFormatVersionUpdate : TableUpdate
{
    [JsonPropertyName("format-version")]
    public int FormatVersion { get; set; }
}

public class AddSchemaUpdate : TableUpdate
{
    [JsonPropertyName("schema")]
    public IcebergSchema Schema { get; set; } = null!;
    [JsonPropertyName("last-column-id")]
    public int? LastColumnId { get; set; }
}

public class SetCurrentSchemaIdUpdate : TableUpdate
{
    [JsonPropertyName("schema-id")]
    public int SchemaId { get; set; }
}

public class AddPartitionSpecUpdate : TableUpdate
{
    [JsonPropertyName("spec")]
    public IcebergPartitionSpec Spec { get; set; } = null!;
}

public class SetDefaultSpecIdUpdate : TableUpdate
{
    [JsonPropertyName("spec-id")]
    public int SpecId { get; set; }
}

public class AddSortOrderUpdate : TableUpdate
{
    [JsonPropertyName("sort-order")]
    public IcebergSortOrder SortOrder { get; set; } = null!;
}

public class SetDefaultSortOrderIdUpdate : TableUpdate
{
    [JsonPropertyName("sort-order-id")]
    public int SortOrderId { get; set; }
}

public class AddSnapshotUpdate : TableUpdate
{
    [JsonPropertyName("snapshot")]
    public IcebergSnapshot Snapshot { get; set; } = null!;
}

public class SetSnapshotRefUpdate : TableUpdate
{
    [JsonPropertyName("ref-name")]
    public string RefName { get; set; } = string.Empty;
    [JsonPropertyName("type")]
    public string Type { get; set; } = "branch";
    [JsonPropertyName("snapshot-id")]
    public long SnapshotId { get; set; }
    [JsonPropertyName("min-snapshots-to-keep")]
    public int? MinSnapshotsToKeep { get; set; }
    [JsonPropertyName("max-snapshot-age-ms")]
    public long? MaxSnapshotAgeMs { get; set; }
    [JsonPropertyName("max-ref-age-ms")]
    public long? MaxRefAgeMs { get; set; }
}

public class RemoveSnapshotsUpdate : TableUpdate
{
    [JsonPropertyName("snapshot-ids")]
    public List<long> SnapshotIds { get; set; } = [];
}

public class RemoveSnapshotRefUpdate : TableUpdate
{
    [JsonPropertyName("ref-name")]
    public string RefName { get; set; } = string.Empty;
}

public class SetLocationUpdate : TableUpdate
{
    [JsonPropertyName("location")]
    public string Location { get; set; } = string.Empty;
}

public class SetPropertiesUpdate : TableUpdate
{
    [JsonPropertyName("updates")]
    public Dictionary<string, string> Updates { get; set; } = [];
}

public class RemovePropertiesUpdate : TableUpdate
{
    [JsonPropertyName("removals")]
    public List<string> Removals { get; set; } = [];
}
