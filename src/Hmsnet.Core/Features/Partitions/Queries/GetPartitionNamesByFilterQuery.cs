using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionNamesByFilterQuery(string DbName, string TableName, string Filter, int MaxParts = -1)
    : IRequest<IReadOnlyList<string>>;
