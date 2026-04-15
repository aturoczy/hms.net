using Hmsnet.Core.Features.ColumnStatistics.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.ColumnStatistics;

public class UpdatePartitionStatsHandler(IColumnStatisticsService svc)
    : IRequestHandler<UpdatePartitionStatsCommand>
{
    public Task Handle(UpdatePartitionStatsCommand request, CancellationToken ct) =>
        svc.UpdatePartitionColumnStatisticsAsync(
            request.DbName, request.TableName, request.PartitionValues, request.Stats, ct);
}
