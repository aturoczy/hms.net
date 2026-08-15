namespace Hmsnet.Iceberg.Models;

// Minimal shapes covering the Iceberg VIEW REST spec. The metadata JSON is
// passed through opaquely — we store what the client sends and return it
// verbatim on load, matching how the tables endpoint handles table metadata.

public record IcebergViewIdentifier(List<string> Namespace, string Name);

public record ListViewsResponse(List<IcebergViewIdentifier> Identifiers);

public record IcebergCreateViewRequest(
    string Name,
    IcebergViewVersion ViewVersion,
    Dictionary<string, string>? Properties = null,
    string? Location = null);

public record IcebergViewVersion(
    int VersionId,
    long TimestampMs,
    string DefaultCatalog,
    List<string> DefaultNamespace,
    List<IcebergViewRepresentation> Representations,
    Dictionary<string, string>? Summary = null);

public record IcebergViewRepresentation(
    string Type,
    string Sql,
    string Dialect);

public record LoadViewResponse(
    string MetadataLocation,
    IcebergViewMetadata Metadata);

public record IcebergViewMetadata(
    string ViewUuid,
    int FormatVersion,
    string Location,
    int CurrentVersionId,
    List<IcebergViewVersion> Versions,
    Dictionary<string, string>? Properties = null);

public record RenameViewRequest(
    IcebergViewIdentifier Source,
    IcebergViewIdentifier Destination);
