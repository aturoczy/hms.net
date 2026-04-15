using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

public record DropPartitionCommand(string DbName, string TableName, List<string> Values, bool DeleteData)
    : IRequest<bool>;
