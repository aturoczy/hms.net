namespace Hmsnet.Core.Models;

public class ColumnStatistics
{
    public int Id { get; set; }
    public string ColumnName { get; set; } = string.Empty;
    public string ColumnType { get; set; } = string.Empty;
    public StatisticsType StatisticsType { get; set; }

    public long LastAnalyzed { get; set; } = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

    // Shared
    public long? NumNulls { get; set; }
    public long? NumDistinctValues { get; set; }

    // Boolean stats
    public long? NumTrues { get; set; }
    public long? NumFalses { get; set; }

    // Long stats
    public long? LongLow { get; set; }
    public long? LongHigh { get; set; }

    // Double stats
    public double? DoubleLow { get; set; }
    public double? DoubleHigh { get; set; }

    // String / Binary stats
    public long? MaxColLen { get; set; }
    public double? AvgColLen { get; set; }

    // Decimal stats (stored as string to avoid precision loss)
    public string? DecimalLow { get; set; }
    public string? DecimalHigh { get; set; }

    // Date stats (Unix epoch days)
    public long? DateLow { get; set; }
    public long? DateHigh { get; set; }

    // Bit vector for HLL estimation (Base64-encoded)
    public string? BitVector { get; set; }

    // FK — stats can belong to a table or a partition
    public int? TableId { get; set; }
    public HiveTable? Table { get; set; }

    public int? PartitionId { get; set; }
    public HivePartition? Partition { get; set; }
}
