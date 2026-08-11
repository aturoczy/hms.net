using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Catalogs.Commands;

public record CreateCatalogCommand(Catalog Catalog)
    : IRequest<Catalog>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTagsExtra.CatalogList, CacheTagsExtra.Catalog(Catalog.Name)];
}

public record AlterCatalogCommand(string Name, Catalog Updated)
    : IRequest<Catalog>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTagsExtra.CatalogList, CacheTagsExtra.Catalog(Name)];
}

public record DropCatalogCommand(string Name) : IRequest, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTagsExtra.CatalogList, CacheTagsExtra.Catalog(Name)];
}
