using Hmsnet.Core.Features.Catalogs.Commands;
using Hmsnet.Core.Features.Catalogs.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Infrastructure.Features.Catalogs;

public class GetAllCatalogsHandler(ICatalogService svc)
    : IRequestHandler<GetAllCatalogsQuery, IReadOnlyList<Catalog>>
{
    public Task<IReadOnlyList<Catalog>> Handle(GetAllCatalogsQuery r, CancellationToken ct) =>
        svc.GetAllCatalogsAsync(ct);
}

public class GetAllCatalogNamesHandler(ICatalogService svc)
    : IRequestHandler<GetAllCatalogNamesQuery, IReadOnlyList<string>>
{
    public Task<IReadOnlyList<string>> Handle(GetAllCatalogNamesQuery r, CancellationToken ct) =>
        svc.GetAllCatalogNamesAsync(ct);
}

public class GetCatalogHandler(ICatalogService svc)
    : IRequestHandler<GetCatalogQuery, Catalog?>
{
    public Task<Catalog?> Handle(GetCatalogQuery r, CancellationToken ct) =>
        svc.GetCatalogAsync(r.Name, ct);
}

public class CatalogExistsHandler(ICatalogService svc)
    : IRequestHandler<CatalogExistsQuery, bool>
{
    public Task<bool> Handle(CatalogExistsQuery r, CancellationToken ct) =>
        svc.CatalogExistsAsync(r.Name, ct);
}

public class CreateCatalogHandler(ICatalogService svc)
    : IRequestHandler<CreateCatalogCommand, Catalog>
{
    public Task<Catalog> Handle(CreateCatalogCommand r, CancellationToken ct) =>
        svc.CreateCatalogAsync(r.Catalog, ct);
}

public class AlterCatalogHandler(ICatalogService svc)
    : IRequestHandler<AlterCatalogCommand, Catalog>
{
    public Task<Catalog> Handle(AlterCatalogCommand r, CancellationToken ct) =>
        svc.AlterCatalogAsync(r.Name, r.Updated, ct);
}

public class DropCatalogHandler(ICatalogService svc) : IRequestHandler<DropCatalogCommand>
{
    public Task Handle(DropCatalogCommand r, CancellationToken ct) =>
        svc.DropCatalogAsync(r.Name, ct);
}
