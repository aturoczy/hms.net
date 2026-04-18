using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record DropDatabaseCommand(string Name, bool Cascade)
    : IRequest, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.DatabaseList, CacheTags.Database(Name), CacheTags.TableList(Name)];
}
