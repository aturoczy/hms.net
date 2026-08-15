namespace Hmsnet.Core.Models;

/// <summary>
/// An Iceberg view — a portable SQL view definition stored in the catalog.
/// Implements the Iceberg VIEW spec (stable since 2025) so multiple engines
/// (Spark, Trino, Snowflake, Dremio, …) can share the same view definition.
///
/// Uniqueness is (<see cref="DatabaseId"/>, <see cref="Name"/>).
/// </summary>
public class IcebergView
{
    public int Id { get; set; }

    /// <summary>View name (lower-cased on write).</summary>
    public string Name { get; set; } = string.Empty;

    public int DatabaseId { get; set; }
    public HiveDatabase Database { get; set; } = null!;

    /// <summary>Absolute location of the current view metadata JSON.</summary>
    public string MetadataLocation { get; set; } = string.Empty;

    /// <summary>The full JSON payload of the current view metadata (spec v1).</summary>
    public string MetadataJson { get; set; } = "{}";

    /// <summary>Monotonically increasing version-id from the metadata (used for optimistic replace).</summary>
    public int CurrentVersionId { get; set; }

    public long CreateTime { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    public long LastAlteredTime { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
}
