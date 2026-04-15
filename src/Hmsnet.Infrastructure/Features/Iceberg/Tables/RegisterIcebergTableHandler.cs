using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Tables;

public class RegisterIcebergTableHandler(IIcebergCatalogService svc)
    : IRequestHandler<RegisterIcebergTableCommand, IcebergTableMetadata>
{
    public Task<IcebergTableMetadata> Handle(RegisterIcebergTableCommand request, CancellationToken ct) =>
        svc.RegisterTableAsync(request.DbName, request.TableName, request.MetadataLocation, request.MetadataJson, ct);
}
