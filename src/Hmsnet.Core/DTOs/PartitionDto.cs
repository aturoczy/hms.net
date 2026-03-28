namespace Hmsnet.Core.DTOs;

public record PartitionRequest(
    List<string> Values,
    StorageDescriptorDto? StorageDescriptor,
    Dictionary<string, string>? Parameters
);

public record PartitionResponse(
    List<string> Values,
    string PartitionName,
    StorageDescriptorDto StorageDescriptor,
    Dictionary<string, string> Parameters,
    long CreateTime,
    long LastAccessTime
);
