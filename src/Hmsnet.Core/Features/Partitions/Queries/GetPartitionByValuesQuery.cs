using Hmsnet.Core.Features.Partitions.Results;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Queries;

public record GetPartitionByValuesQuery(string DbName, string TableName, List<string> Values)
    : IRequest<PartitionWithKeysResult?>;
