namespace Hmsnet.Core.DTOs;

public record SortOrderDto(string Column, int Order);

public record StorageDescriptorDto(
    string Location,
    string InputFormat,
    string OutputFormat,
    bool Compressed,
    int NumBuckets,
    SerDeInfoDto SerDeInfo,
    List<string>? BucketColumns,
    List<SortOrderDto>? SortColumns,
    Dictionary<string, string>? Parameters,
    bool StoredAsSubDirectories = false
);
