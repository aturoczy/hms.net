namespace Hmsnet.Core.Models;

public class HiveDatabase
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Description { get; set; }
    public string LocationUri { get; set; } = string.Empty;
    public string? OwnerName { get; set; }
    public PrincipalType OwnerType { get; set; } = PrincipalType.User;
    public Dictionary<string, string> Parameters { get; set; } = [];
    public long CreateTime { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    /// <summary>
    /// Name of the parent <see cref="Catalog"/>. Defaults to
    /// <see cref="Catalog.DefaultName"/> so pre-3.x deployments keep working.
    /// </summary>
    public string CatalogName { get; set; } = Catalog.DefaultName;

    public ICollection<HiveTable> Tables { get; set; } = [];
}
