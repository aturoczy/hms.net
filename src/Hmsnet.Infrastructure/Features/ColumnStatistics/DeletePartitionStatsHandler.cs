using Hmsnet.Core.Features.ColumnStatistics.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.ColumnStatistics;

public class DeletePartitionStatsHandler(IColumnStatisticsService svc)
    : IRequestHandler<DeletePartitionStatsCommand>
{
    public Task Handle(DeletePartitionStatsCommand request, CancellationToken ct) =>
        svc.DeletePartitionColumnStatisticsAsync(
            request.DbName, request.TableName, request.PartitionValues, request.Column, ct);
}
