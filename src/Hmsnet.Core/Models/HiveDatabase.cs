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

    public ICollection<HiveTable> Tables { get; set; } = [];
}
