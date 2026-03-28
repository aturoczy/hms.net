namespace Hmsnet.Core.Models;

public class HivePartition
{
    public int Id { get; set; }

    public int TableId { get; set; }
    public HiveTable Table { get; set; } = null!;

    /// <summary>
    /// JSON-serialized partition values stored in DB — used for EF queries.
    /// Use <see cref="Values"/> for application code.
    /// </summary>
    public string ValuesJson { get; set; } = "[]";

    /// <summary>
    /// Ordered list of partition values matching the table's partition key order.
    /// e.g. ["2024", "01", "15"] for dt=2024/month=01/day=15
    /// </summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public List<string> Values
    {
        get => System.Text.Json.JsonSerializer.Deserialize<List<string>>(ValuesJson) ?? [];
        set => ValuesJson = System.Text.Json.JsonSerializer.Serialize(value);
    }

    public long CreateTime { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    public long LastAccessTime { get; set; } = 0;

    public int StorageDescriptorId { get; set; }
    public StorageDescriptor StorageDescriptor { get; set; } = new();

    public Dictionary<string, string> Parameters { get; set; } = [];

    public ICollection<ColumnStatistics> ColumnStatistics { get; set; } = [];
}
