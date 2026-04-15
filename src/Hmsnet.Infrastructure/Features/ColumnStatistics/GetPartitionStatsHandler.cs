using Hmsnet.Core.Features.ColumnStatistics.Queries;
using Hmsnet.Core.Interfaces;
using MediatR;
using ColStats = Hmsnet.Core.Models.ColumnStatistics;

namespace Hmsnet.Infrastructure.Features.ColumnStatistics;

public class GetPartitionStatsHandler(IColumnStatisticsService svc)
    : IRequestHandler<GetPartitionStatsQuery, IReadOnlyList<ColStats>>
{
    public Task<IReadOnlyList<ColStats>> Handle(GetPartitionStatsQuery request, CancellationToken ct) =>
        svc.GetPartitionColumnStatisticsAsync(
            request.DbName, request.TableName, request.PartitionValues, request.Columns, ct);
}
