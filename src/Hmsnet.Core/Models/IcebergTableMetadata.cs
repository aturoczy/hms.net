namespace Hmsnet.Core.Models;

public class IcebergTableMetadata
{
    public int Id { get; set; }
    public int HiveTableId { get; set; }
    public HiveTable HiveTable { get; set; } = null!;

    /// <summary>Logical metadata file location, e.g. s3://bucket/table/metadata/v2.metadata.json</summary>
    public string MetadataLocation { get; set; } = string.Empty;

    /// <summary>Full serialized Iceberg table metadata V2 JSON.</summary>
    public string MetadataJson { get; set; } = "{}";
}
