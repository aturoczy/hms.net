using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionNamesQuery(string DbName, string TableName, int MaxParts = -1)
    : IRequest<IReadOnlyList<string>>;
