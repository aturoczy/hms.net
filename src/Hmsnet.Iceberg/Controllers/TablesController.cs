using System.Text.Json;
using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Features.Iceberg.Queries;
using Hmsnet.Core.Models;
using Hmsnet.Iceberg.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Iceberg.Controllers;

[ApiController]
public class TablesController(ISender sender) : IcebergControllerBase
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = null,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    // ── List tables ───────────────────────────────────────────────────────────

    [HttpGet("v1/namespaces/{ns}/tables")]
    public async Task<IActionResult> ListTables(string ns, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        var dbName = NamespaceToDatabaseName(parts);
        return await HandleAsync(async () =>
        {
            var tables = await sender.Send(new ListIcebergTablesQuery(dbName), ct);
            var identifiers = tables
                .Select(t => new IcebergTableIdentifier([dbName], t.Name))
                .ToList();
            return Ok(new ListTablesResponse(identifiers));
        });
    }

    // ── Create table ──────────────────────────────────────────────────────────

    [HttpPost("v1/namespaces/{ns}/tables")]
    public async Task<IActionResult> CreateTable(
        string ns, [FromBody] IcebergCreateTableRequest request, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        var dbName = NamespaceToDatabaseName(parts);

        return await HandleAsync(async () =>
        {
            var location = request.Location ?? $"/{dbName}/{request.Name}";
            var metadata = BuildInitialMetadata(request, location);
            var metaJson = JsonSerializer.Serialize(metadata, JsonOpts);
            var metaLocation = $"{location}/metadata/v1.metadata.json";

            var hiveTable = BuildHiveTable(request, dbName, location);

            var iceMeta = await sender.Send(
                new CreateIcebergTableCommand(dbName, hiveTable, metaLocation, metaJson), ct);

            var loadResponse = BuildLoadTableResponse(iceMeta);
            return Ok(loadResponse);
        });
    }

    // ── Load table ────────────────────────────────────────────────────────────

    [HttpGet("v1/namespaces/{ns}/tables/{table}")]
    public async Task<IActionResult> LoadTable(string ns, string table, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        var dbName = NamespaceToDatabaseName(parts);
        var meta = await sender.Send(new LoadIcebergTableQuery(dbName, table), ct);
        if (meta is null) return IcebergTableNotFound($"Table '{dbName}.{table}' does not exist.");
        return Ok(BuildLoadTableResponse(meta));
    }

    // ── Table exists ──────────────────────────────────────────────────────────

    [HttpHead("v1/namespaces/{ns}/tables/{table}")]
    public async Task<IActionResult> TableExists(string ns, string table, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        var dbName = NamespaceToDatabaseName(parts);
        var meta = await sender.Send(new LoadIcebergTableQuery(dbName, table), ct);
        return meta is null ? IcebergTableNotFound($"Table '{dbName}.{table}' does not exist.") : Ok();
    }

    // ── Commit table ──────────────────────────────────────────────────────────

    [HttpPost("v1/namespaces/{ns}/tables/{table}")]
    public async Task<IActionResult> CommitTable(
        string ns, string table, [FromBody] IcebergCommitTableRequest request, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        var dbName = NamespaceToDatabaseName(parts);

        return await HandleAsync(async () =>
        {
            // Load current metadata
            var current = await sender.Send(new LoadIcebergTableQuery(dbName, table), ct);
            if (current is null) return IcebergTableNotFound($"Table '{dbName}.{table}' does not exist.");

            var metadata = JsonSerializer.Deserialize<IcebergTableMetadataV2>(current.MetadataJson, JsonOpts)
                ?? throw new InvalidOperationException("Failed to deserialize table metadata.");

            // Verify requirements
            var requirementError = CheckRequirements(request.Requirements, metadata);
            if (requirementError is not null) return requirementError;

            // Apply updates
            ApplyUpdates(metadata, request.Updates);
            metadata.LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // Bump metadata version
            var baseLocation = current.MetadataLocation;
            var versionNumber = ExtractVersionNumber(baseLocation) + 1;
            var tableLocation = metadata.Location;
            var newMetaLocation = $"{tableLocation}/metadata/v{versionNumber}.metadata.json";

            // Add metadata-log entry
            metadata.MetadataLog.Add(new IcebergMetadataLogEntry(current.MetadataLocation, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));

            var newMetaJson = JsonSerializer.Serialize(metadata, JsonOpts);
            var committed = await sender.Send(
                new CommitIcebergTableCommand(dbName, table, newMetaLocation, newMetaJson), ct);

            return Ok(new IcebergCommitTableResponse(committed.MetadataLocation,
                JsonSerializer.Deserialize<IcebergTableMetadataV2>(committed.MetadataJson, JsonOpts)!));
        });
    }

    // ── Drop table ────────────────────────────────────────────────────────────

    [HttpDelete("v1/namespaces/{ns}/tables/{table}")]
    public async Task<IActionResult> DropTable(
        string ns, string table,
        [FromQuery] bool purgeRequested = false,
        CancellationToken ct = default)
    {
        var parts = DecodeNamespace(ns);
        var dbName = NamespaceToDatabaseName(parts);
        return await HandleAsync(async () =>
        {
            await sender.Send(new DropIcebergTableCommand(dbName, table, purgeRequested), ct);
            return NoContent();
        });
    }

    // ── Rename table ──────────────────────────────────────────────────────────

    [HttpPost("v1/tables/rename")]
    public async Task<IActionResult> RenameTable([FromBody] RenameTableRequest request, CancellationToken ct)
    {
        return await HandleAsync(async () =>
        {
            var fromDb = request.Source.Namespace.Count > 0
                ? request.Source.Namespace[0].ToLowerInvariant()
                : throw new Hmsnet.Core.Exceptions.InvalidOperationException("Source namespace is required.");
            var toDb = request.Destination.Namespace.Count > 0
                ? request.Destination.Namespace[0].ToLowerInvariant()
                : throw new Hmsnet.Core.Exceptions.InvalidOperationException("Destination namespace is required.");

            await sender.Send(new RenameIcebergTableCommand(
                fromDb, request.Source.Name, toDb, request.Destination.Name), ct);
            return NoContent();
        });
    }

    // ── Register table ────────────────────────────────────────────────────────

    [HttpPost("v1/namespaces/{ns}/register")]
    public async Task<IActionResult> RegisterTable(
        string ns, [FromBody] RegisterTableRequest request, CancellationToken ct)
    {
        var parts = DecodeNamespace(ns);
        var dbName = NamespaceToDatabaseName(parts);

        return await HandleAsync(async () =>
        {
            // We store the metadata location but read the actual metadata JSON from the request
            // In a real catalog backed by object storage, we would fetch the file.
            // Here we store a minimal valid metadata stub.
            var metadataJson = JsonSerializer.Serialize(new IcebergTableMetadataV2
            {
                Location = $"/{dbName}/{request.Name}",
                TableUuid = Guid.NewGuid().ToString()
            }, JsonOpts);

            var iceMeta = await sender.Send(
                new RegisterIcebergTableCommand(dbName, request.Name, request.MetadataLocation, metadataJson), ct);
            return Ok(BuildLoadTableResponse(iceMeta));
        });
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private static IcebergLoadTableResponse BuildLoadTableResponse(Core.Models.IcebergTableMetadata meta)
    {
        var metadata = JsonSerializer.Deserialize<IcebergTableMetadataV2>(meta.MetadataJson, JsonOpts)
            ?? new IcebergTableMetadataV2();
        return new IcebergLoadTableResponse(meta.MetadataLocation, metadata);
    }

    private static IcebergTableMetadataV2 BuildInitialMetadata(IcebergCreateTableRequest request, string location)
    {
        var schema = request.Schema with { SchemaId = 0 };
        var partitionSpec = request.PartitionSpec ?? new IcebergPartitionSpec(0, []);
        var sortOrder = request.WriteOrder ?? new IcebergSortOrder(0, []);

        return new IcebergTableMetadataV2
        {
            TableUuid = Guid.NewGuid().ToString(),
            Location = location,
            LastColumnId = schema.Fields.Count > 0 ? schema.Fields.Max(f => f.Id) : 0,
            CurrentSchemaId = 0,
            Schemas = [schema],
            DefaultSpecId = 0,
            PartitionSpecs = [partitionSpec],
            LastPartitionId = partitionSpec.Fields.Count > 0 ? partitionSpec.Fields.Max(f => f.FieldId) : 999,
            DefaultSortOrderId = 0,
            SortOrders = [sortOrder],
            Properties = request.Properties ?? [],
            CurrentSnapshotId = null,
            Snapshots = [],
            SnapshotLog = [],
            MetadataLog = []
        };
    }

    private static HiveTable BuildHiveTable(IcebergCreateTableRequest request, string dbName, string location)
    {
        var cols = request.Schema.Fields.Select((f, i) =>
        {
            var typeName = f.Type.ValueKind == System.Text.Json.JsonValueKind.String
                ? f.Type.GetString() ?? "string"
                : f.Type.ToString();
            return new HiveColumn
            {
                Name = f.Name,
                TypeName = typeName,
                OrdinalPosition = i,
                IsPartitionKey = false
            };
        }).ToList();

        return new HiveTable
        {
            Name = request.Name,
            TableType = TableType.ExternalTable,
            Parameters = new Dictionary<string, string>(request.Properties ?? [])
            {
                ["table_type"] = "ICEBERG"
            },
            StorageDescriptor = new StorageDescriptor
            {
                Location = location,
                InputFormat = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
                OutputFormat = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat",
                SerDeInfo = new SerDeInfo
                {
                    SerializationLib = "org.apache.iceberg.mr.hive.HiveIcebergSerDe"
                }
            },
            Columns = cols,
            Database = new HiveDatabase { Name = dbName }
        };
    }

    private IActionResult? CheckRequirements(
        List<TableRequirement> requirements, IcebergTableMetadataV2 metadata)
    {
        foreach (var req in requirements)
        {
            switch (req)
            {
                case AssertTableUuid r when !string.Equals(r.Uuid, metadata.TableUuid, StringComparison.OrdinalIgnoreCase):
                    return Conflict(new IcebergErrorResponse(new IcebergErrorModel(
                        $"Table UUID does not match: expected {r.Uuid}, got {metadata.TableUuid}",
                        "CommitFailedException", 409)));

                case AssertCurrentSchemaId r when r.CurrentSchemaId != metadata.CurrentSchemaId:
                    return Conflict(new IcebergErrorResponse(new IcebergErrorModel(
                        $"Current schema ID does not match: expected {r.CurrentSchemaId}, got {metadata.CurrentSchemaId}",
                        "CommitFailedException", 409)));

                case AssertDefaultSpecId r when r.DefaultSpecId != metadata.DefaultSpecId:
                    return Conflict(new IcebergErrorResponse(new IcebergErrorModel(
                        $"Default spec ID does not match: expected {r.DefaultSpecId}, got {metadata.DefaultSpecId}",
                        "CommitFailedException", 409)));

                case AssertDefaultSortOrderId r when r.DefaultSortOrderId != metadata.DefaultSortOrderId:
                    return Conflict(new IcebergErrorResponse(new IcebergErrorModel(
                        $"Default sort order ID does not match: expected {r.DefaultSortOrderId}, got {metadata.DefaultSortOrderId}",
                        "CommitFailedException", 409)));

                case AssertLastAssignedFieldId r when r.LastAssignedFieldId != metadata.LastColumnId:
                    return Conflict(new IcebergErrorResponse(new IcebergErrorModel(
                        $"Last assigned field ID does not match: expected {r.LastAssignedFieldId}, got {metadata.LastColumnId}",
                        "CommitFailedException", 409)));

                case AssertLastAssignedPartitionId r when r.LastAssignedPartitionId != metadata.LastPartitionId:
                    return Conflict(new IcebergErrorResponse(new IcebergErrorModel(
                        $"Last assigned partition ID does not match: expected {r.LastAssignedPartitionId}, got {metadata.LastPartitionId}",
                        "CommitFailedException", 409)));

                case AssertRefSnapshotId r:
                {
                    var refName = r.Ref ?? "main";
                    IcebergSnapshotRef? currentRef = null;
                    metadata.Refs?.TryGetValue(refName, out currentRef);
                    var currentSnapshotId = currentRef?.SnapshotId ?? metadata.CurrentSnapshotId;
                    if (currentSnapshotId != r.SnapshotId)
                        return Conflict(new IcebergErrorResponse(new IcebergErrorModel(
                            $"Ref '{refName}' snapshot ID does not match: expected {r.SnapshotId}, got {currentSnapshotId}",
                            "CommitFailedException", 409)));
                    break;
                }
            }
        }
        return null;
    }

    private static void ApplyUpdates(IcebergTableMetadataV2 metadata, List<TableUpdate> updates)
    {
        foreach (var update in updates)
        {
            switch (update)
            {
                case UpgradeFormatVersionUpdate u:
                    metadata.FormatVersion = u.FormatVersion;
                    break;

                case AddSchemaUpdate u:
                    var schemaId = u.Schema.SchemaId >= 0 ? u.Schema.SchemaId : (metadata.Schemas.Count > 0 ? metadata.Schemas.Max(s => s.SchemaId) + 1 : 0);
                    var newSchema = u.Schema with { SchemaId = schemaId };
                    metadata.Schemas.Add(newSchema);
                    if (u.LastColumnId.HasValue) metadata.LastColumnId = u.LastColumnId.Value;
                    break;

                case SetCurrentSchemaIdUpdate u:
                    metadata.CurrentSchemaId = u.SchemaId == -1
                        ? (metadata.Schemas.Count > 0 ? metadata.Schemas.Max(s => s.SchemaId) : 0)
                        : u.SchemaId;
                    break;

                case AddPartitionSpecUpdate u:
                    metadata.PartitionSpecs.Add(u.Spec);
                    if (u.Spec.Fields.Count > 0)
                        metadata.LastPartitionId = Math.Max(metadata.LastPartitionId, u.Spec.Fields.Max(f => f.FieldId));
                    break;

                case SetDefaultSpecIdUpdate u:
                    metadata.DefaultSpecId = u.SpecId == -1
                        ? (metadata.PartitionSpecs.Count > 0 ? metadata.PartitionSpecs.Max(s => s.SpecId) : 0)
                        : u.SpecId;
                    break;

                case AddSortOrderUpdate u:
                    metadata.SortOrders.Add(u.SortOrder);
                    break;

                case SetDefaultSortOrderIdUpdate u:
                    metadata.DefaultSortOrderId = u.SortOrderId == -1
                        ? (metadata.SortOrders.Count > 0 ? metadata.SortOrders.Max(s => s.OrderId) : 0)
                        : u.SortOrderId;
                    break;

                case AddSnapshotUpdate u:
                    metadata.Snapshots.Add(u.Snapshot);
                    metadata.LastSequenceNumber = Math.Max(metadata.LastSequenceNumber, u.Snapshot.SequenceNumber);
                    metadata.SnapshotLog.Add(new IcebergSnapshotLogEntry(u.Snapshot.SnapshotId, u.Snapshot.TimestampMs));
                    break;

                case SetSnapshotRefUpdate u:
                    metadata.Refs ??= [];
                    metadata.Refs[u.RefName] = new IcebergSnapshotRef(
                        u.SnapshotId, u.Type, u.MinSnapshotsToKeep, u.MaxSnapshotAgeMs, u.MaxRefAgeMs);
                    if (u.RefName == "main")
                        metadata.CurrentSnapshotId = u.SnapshotId;
                    break;

                case RemoveSnapshotsUpdate u:
                    metadata.Snapshots.RemoveAll(s => u.SnapshotIds.Contains(s.SnapshotId));
                    break;

                case RemoveSnapshotRefUpdate u:
                    metadata.Refs?.Remove(u.RefName);
                    break;

                case SetLocationUpdate u:
                    metadata.Location = u.Location;
                    break;

                case SetPropertiesUpdate u:
                    foreach (var (k, v) in u.Updates)
                        metadata.Properties[k] = v;
                    break;

                case RemovePropertiesUpdate u:
                    foreach (var k in u.Removals)
                        metadata.Properties.Remove(k);
                    break;
            }
        }
    }

    private static int ExtractVersionNumber(string metadataLocation)
    {
        // Extract N from ".../metadata/vN.metadata.json"
        var fileName = System.IO.Path.GetFileNameWithoutExtension(metadataLocation);
        // fileName is e.g. "v2.metadata" — strip leading 'v' and trailing ".metadata"
        var withoutMeta = fileName.EndsWith(".metadata") ? fileName[..^9] : fileName;
        if (withoutMeta.StartsWith('v') && int.TryParse(withoutMeta[1..], out var n))
            return n;
        return 1;
    }
}
