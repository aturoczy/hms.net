using Hmsnet.Core.Features.Partitions.Results;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionsQuery(string DbName, string TableName, int MaxParts = -1)
    : IRequest<PartitionsWithKeysResult>;
