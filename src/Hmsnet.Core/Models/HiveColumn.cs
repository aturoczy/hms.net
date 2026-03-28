namespace Hmsnet.Core.Models;

public class HiveColumn
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Hive type string, e.g. "string", "int", "array&lt;string&gt;", "map&lt;string,int&gt;", "struct&lt;a:int,b:string&gt;"
    /// </summary>
    public string TypeName { get; set; } = string.Empty;

    public string? Comment { get; set; }
    public int OrdinalPosition { get; set; }

    // FK - column belongs to a table (data column or partition key)
    public int TableId { get; set; }
    public HiveTable Table { get; set; } = null!;
    public bool IsPartitionKey { get; set; } = false;
}
