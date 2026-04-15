using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Tables;

public class CreateIcebergTableHandler(IIcebergCatalogService svc)
    : IRequestHandler<CreateIcebergTableCommand, IcebergTableMetadata>
{
    public Task<IcebergTableMetadata> Handle(CreateIcebergTableCommand request, CancellationToken ct) =>
        svc.CreateTableAsync(request.DbName, request.Table, request.MetadataLocation, request.MetadataJson, ct);
}
