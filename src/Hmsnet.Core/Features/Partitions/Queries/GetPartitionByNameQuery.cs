using Hmsnet.Core.Features.Partitions.Results;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionByNameQuery(string DbName, string TableName, string PartitionName)
    : IRequest<PartitionWithKeysResult?>;
