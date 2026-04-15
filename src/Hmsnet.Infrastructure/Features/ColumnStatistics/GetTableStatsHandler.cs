using Hmsnet.Core.Features.ColumnStatistics.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;
using ColStats = Hmsnet.Core.Models.ColumnStatistics;

namespace Hmsnet.Infrastructure.Features.ColumnStatistics;

public class GetTableStatsHandler(IColumnStatisticsService svc)
    : IRequestHandler<GetTableStatsQuery, IReadOnlyList<ColStats>>
{
    public Task<IReadOnlyList<ColStats>> Handle(GetTableStatsQuery request, CancellationToken ct) =>
        svc.GetTableColumnStatisticsAsync(request.DbName, request.TableName, request.Columns, ct);
}
