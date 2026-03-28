namespace Hmsnet.Core.DTOs;

public record TableRequest(
    string Name,
    string DatabaseName,
    string? Owner,
    string TableType,
    StorageDescriptorDto StorageDescriptor,
    List<ColumnDto> Columns,
    List<ColumnDto>? PartitionKeys,
    string? ViewOriginalText,
    string? ViewExpandedText,
    Dictionary<string, string>? Parameters,
    bool Temporary = false
);

public record TableResponse(
    string Name,
    string DatabaseName,
    string? Owner,
    string TableType,
    StorageDescriptorDto StorageDescriptor,
    List<ColumnDto> Columns,
    List<ColumnDto> PartitionKeys,
    string? ViewOriginalText,
    string? ViewExpandedText,
    Dictionary<string, string> Parameters,
    bool Temporary,
    long CreateTime,
    long LastAccessTime,
    int Retention
);
