using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.ColumnStatistics.Queries;

public record GetTableStatsQuery(string DbName, string TableName, List<string> Columns)
    : IRequest<IReadOnlyList<Hmsnet.Core.Models.ColumnStatistics>>;
