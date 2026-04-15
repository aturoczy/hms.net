using Hmsnet.Core.Features.Iceberg.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Tables;

public class LoadIcebergTableHandler(IIcebergCatalogService svc)
    : IRequestHandler<LoadIcebergTableQuery, IcebergTableMetadata?>
{
    public Task<IcebergTableMetadata?> Handle(LoadIcebergTableQuery request, CancellationToken ct) =>
        svc.LoadTableAsync(request.DbName, request.TableName, ct);
}
