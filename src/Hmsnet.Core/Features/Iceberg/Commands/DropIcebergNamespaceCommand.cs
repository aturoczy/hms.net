using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record DropIcebergNamespaceCommand(string Name) : IRequest, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.IcebergNamespaceList, CacheTags.DatabaseList, CacheTags.Database(Name), CacheTags.TableList(Name)];
}
