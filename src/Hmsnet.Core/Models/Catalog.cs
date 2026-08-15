namespace Hmsnet.Core.Models;

/// <summary>
/// Top-level namespace above <see cref="HiveDatabase"/>. Added to Hive
/// Metastore in 3.x; Hive 4.2 formalised the "catalog properties" API.
/// A default catalog named <c>hive</c> is seeded so existing single-catalog
/// deployments keep working without changes.
/// </summary>
public class Catalog
{
    public int Id { get; set; }

    /// <summary>Catalog name — unique. Referenced from HiveDatabase.CatalogName.</summary>
    public string Name { get; set; } = string.Empty;

    public string? Description { get; set; }

    /// <summary>Root storage URI for objects in this catalog.</summary>
    public string LocationUri { get; set; } = string.Empty;

    /// <summary>Free-form key/value properties attached to the catalog itself.</summary>
    public Dictionary<string, string> Properties { get; set; } = [];

    public long CreateTime { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    public ICollection<HiveDatabase> Databases { get; set; } = [];

    /// <summary>Default catalog name that pre-existing databases belong to.</summary>
    public const string DefaultName = "hive";
}
