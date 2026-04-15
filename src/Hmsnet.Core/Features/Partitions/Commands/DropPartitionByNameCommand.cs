using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

public record DropPartitionByNameCommand(string DbName, string TableName, string PartitionName, bool DeleteData)
    : IRequest<bool>;
