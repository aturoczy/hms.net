using Hmsnet.Core.Features.ColumnStatistics.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.ColumnStatistics;

public class UpdateTableStatsHandler(IColumnStatisticsService svc) : IRequestHandler<UpdateTableStatsCommand>
{
    public Task Handle(UpdateTableStatsCommand request, CancellationToken ct) =>
        svc.UpdateTableColumnStatisticsAsync(request.DbName, request.TableName, request.Stats, ct);
}
