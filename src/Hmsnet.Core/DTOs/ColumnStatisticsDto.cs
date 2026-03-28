namespace Hmsnet.Core.DTOs;

public record ColumnStatisticsDto(
    string ColumnName,
    string ColumnType,
    string StatisticsType,
    long LastAnalyzed,
    long? NumNulls,
    long? NumDistinctValues,
    // Boolean
    long? NumTrues,
    long? NumFalses,
    // Long
    long? LongLow,
    long? LongHigh,
    // Double
    double? DoubleLow,
    double? DoubleHigh,
    // String / Binary
    long? MaxColLen,
    double? AvgColLen,
    // Decimal
    string? DecimalLow,
    string? DecimalHigh,
    // Date (Unix epoch days)
    long? DateLow,
    long? DateHigh,
    string? BitVector
);
