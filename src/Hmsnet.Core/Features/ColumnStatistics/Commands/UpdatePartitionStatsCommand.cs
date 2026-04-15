using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.ColumnStatistics.Commands;

public record UpdatePartitionStatsCommand(
    string DbName, string TableName,
    IList<string> PartitionValues,
    IEnumerable<Hmsnet.Core.Models.ColumnStatistics> Stats) : IRequest;
