using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Interfaces;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Tables;

public class RenameIcebergTableHandler(IIcebergCatalogService svc)
    : IRequestHandler<RenameIcebergTableCommand>
{
    public Task Handle(RenameIcebergTableCommand request, CancellationToken ct) =>
        svc.RenameTableAsync(request.FromDb, request.FromTable, request.ToDb, request.ToTable, ct);
}
