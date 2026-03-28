namespace Hmsnet.Core.DTOs;

public record ColumnDto(
    string Name,
    string TypeName,
    string? Comment,
    int OrdinalPosition,
    bool IsPartitionKey
);
