namespace Hmsnet.Core.Models;

public class HiveTable
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;

    public int DatabaseId { get; set; }
    public HiveDatabase Database { get; set; } = null!;

    public string? Owner { get; set; }
    public long CreateTime { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    public long LastAccessTime { get; set; } = 0;
    public int Retention { get; set; } = 0;

    public TableType TableType { get; set; } = TableType.ManagedTable;

    public int StorageDescriptorId { get; set; }
    public StorageDescriptor StorageDescriptor { get; set; } = new();

    /// <summary>All columns — both data columns and partition keys (distinguished by IsPartitionKey).</summary>
    public ICollection<HiveColumn> Columns { get; set; } = [];

    /// <summary>Partition key columns — computed from Columns where IsPartitionKey == true.</summary>
    [System.Text.Json.Serialization.JsonIgnore]
    public IEnumerable<HiveColumn> PartitionKeys => Columns.Where(c => c.IsPartitionKey);

    public string? ViewOriginalText { get; set; }
    public string? ViewExpandedText { get; set; }

    public Dictionary<string, string> Parameters { get; set; } = [];
    public bool Temporary { get; set; } = false;
    public bool RewriteEnabled { get; set; } = false;

    public ICollection<HivePartition> Partitions { get; set; } = [];
}
