using Hmsnet.Core.Features.ColumnStatistics.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.ColumnStatistics;

public class DeleteTableStatsHandler(IColumnStatisticsService svc) : IRequestHandler<DeleteTableStatsCommand>
{
    public Task Handle(DeleteTableStatsCommand request, CancellationToken ct) =>
        svc.DeleteTableColumnStatisticsAsync(request.DbName, request.TableName, request.Column, ct);
}
