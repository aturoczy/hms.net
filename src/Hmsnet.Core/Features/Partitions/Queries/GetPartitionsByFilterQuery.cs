using Hmsnet.Core.Features.Partitions.Results;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionsByFilterQuery(string DbName, string TableName, string Filter, int MaxParts = -1)
    : IRequest<PartitionsWithKeysResult>;
