using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Tables;

public class DropIcebergTableHandler(IIcebergCatalogService svc)
    : IRequestHandler<DropIcebergTableCommand>
{
    public Task Handle(DropIcebergTableCommand request, CancellationToken ct) =>
        svc.DropTableAsync(request.DbName, request.TableName, request.Purge, ct);
}
