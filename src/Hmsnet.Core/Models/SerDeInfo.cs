namespace Hmsnet.Core.Models;

public class SerDeInfo
{
    public int Id { get; set; }
    public string? Name { get; set; }

    /// <summary>
    /// Fully qualified class name of the SerDe, e.g. "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    /// </summary>
    public string SerializationLib { get; set; } = string.Empty;

    public Dictionary<string, string> Parameters { get; set; } = [];

    public int StorageDescriptorId { get; set; }
    public StorageDescriptor StorageDescriptor { get; set; } = null!;
}
