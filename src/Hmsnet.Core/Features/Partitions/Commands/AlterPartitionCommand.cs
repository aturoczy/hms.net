using Hmsnet.Core.Features.Partitions.Results;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Partitions.Commands;

public record AlterPartitionCommand(string DbName, string TableName, HivePartition Updated)
    : IRequest<PartitionWithKeysResult>;
