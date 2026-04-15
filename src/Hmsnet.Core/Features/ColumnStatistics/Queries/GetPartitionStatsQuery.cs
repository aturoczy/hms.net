using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.ColumnStatistics.Queries;

public record GetPartitionStatsQuery(
    string DbName, string TableName,
    List<string> PartitionValues,
    List<string> Columns) : IRequest<IReadOnlyList<Hmsnet.Core.Models.ColumnStatistics>>;
