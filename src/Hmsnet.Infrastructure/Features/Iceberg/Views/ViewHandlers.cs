using Hmsnet.Core.Features.Iceberg.Views;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Iceberg.Views;

public class ListIcebergViewsHandler(IIcebergViewService svc)
    : IRequestHandler<ListIcebergViewsQuery, IReadOnlyList<IcebergView>>
{
    public Task<IReadOnlyList<IcebergView>> Handle(ListIcebergViewsQuery r, CancellationToken ct) =>
        svc.ListViewsAsync(r.DbName, ct);
}

public class LoadIcebergViewHandler(IIcebergViewService svc)
    : IRequestHandler<LoadIcebergViewQuery, IcebergView?>
{
    public Task<IcebergView?> Handle(LoadIcebergViewQuery r, CancellationToken ct) =>
        svc.LoadViewAsync(r.DbName, r.ViewName, ct);
}

public class IcebergViewExistsHandler(IIcebergViewService svc)
    : IRequestHandler<IcebergViewExistsQuery, bool>
{
    public Task<bool> Handle(IcebergViewExistsQuery r, CancellationToken ct) =>
        svc.ViewExistsAsync(r.DbName, r.ViewName, ct);
}

public class CreateIcebergViewHandler(IIcebergViewService svc)
    : IRequestHandler<CreateIcebergViewCommand, IcebergView>
{
    public Task<IcebergView> Handle(CreateIcebergViewCommand r, CancellationToken ct) =>
        svc.CreateViewAsync(r.DbName, r.ViewName, r.MetadataLocation, r.MetadataJson, r.CurrentVersionId, ct);
}

public class ReplaceIcebergViewHandler(IIcebergViewService svc)
    : IRequestHandler<ReplaceIcebergViewCommand, IcebergView>
{
    public Task<IcebergView> Handle(ReplaceIcebergViewCommand r, CancellationToken ct) =>
        svc.ReplaceViewAsync(r.DbName, r.ViewName, r.NewMetadataLocation, r.NewMetadataJson, r.NewVersionId, ct);
}

public class DropIcebergViewHandler(IIcebergViewService svc) : IRequestHandler<DropIcebergViewCommand>
{
    public Task Handle(DropIcebergViewCommand r, CancellationToken ct) =>
        svc.DropViewAsync(r.DbName, r.ViewName, ct);
}

public class RenameIcebergViewHandler(IIcebergViewService svc) : IRequestHandler<RenameIcebergViewCommand>
{
    public Task Handle(RenameIcebergViewCommand r, CancellationToken ct) =>
        svc.RenameViewAsync(r.FromDb, r.FromView, r.ToDb, r.ToView, ct);
}
