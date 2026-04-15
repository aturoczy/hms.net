using MediatR;

namespace Hmsnet.Core.Features.ColumnStatistics.Commands;

public record DeletePartitionStatsCommand(
    string DbName, string TableName,
    IList<string> PartitionValues,
    string? Column) : IRequest;
