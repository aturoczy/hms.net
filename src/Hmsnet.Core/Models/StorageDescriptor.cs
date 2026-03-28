namespace Hmsnet.Core.Models;

/// <summary>
/// Describes how a table or partition's data is stored on disk.
/// Mirrors Hive's StorageDescriptor Thrift struct.
/// </summary>
public class StorageDescriptor
{
    public int Id { get; set; }

    public string Location { get; set; } = string.Empty;

    /// <summary>e.g. "org.apache.hadoop.mapred.TextInputFormat"</summary>
    public string InputFormat { get; set; } = string.Empty;

    /// <summary>e.g. "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"</summary>
    public string OutputFormat { get; set; } = string.Empty;

    public bool Compressed { get; set; } = false;
    public int NumBuckets { get; set; } = -1;
    public List<string> BucketColumns { get; set; } = [];
    public List<SortOrder> SortColumns { get; set; } = [];
    public Dictionary<string, string> Parameters { get; set; } = [];
    public SkewedInfo SkewedInfo { get; set; } = new();
    public bool StoredAsSubDirectories { get; set; } = false;

    public SerDeInfo SerDeInfo { get; set; } = new();

    // Back-references (one SD can be owned by a table or a partition)
    public HiveTable? Table { get; set; }
    public HivePartition? Partition { get; set; }
}
