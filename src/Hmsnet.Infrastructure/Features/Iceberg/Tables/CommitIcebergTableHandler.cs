using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Tables;

public class CommitIcebergTableHandler(IIcebergCatalogService svc)
    : IRequestHandler<CommitIcebergTableCommand, IcebergTableMetadata>
{
    public Task<IcebergTableMetadata> Handle(CommitIcebergTableCommand request, CancellationToken ct) =>
        svc.CommitTableAsync(request.DbName, request.TableName, request.NewMetadataLocation, request.NewMetadataJson, ct);
}
