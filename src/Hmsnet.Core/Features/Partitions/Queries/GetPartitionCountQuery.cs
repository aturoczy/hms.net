using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionCountQuery(string DbName, string TableName) : IRequest<int>;
