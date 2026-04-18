using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record CreateDatabaseCommand(HiveDatabase Database)
    : IRequest<HiveDatabase>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.DatabaseList, CacheTags.Database(Database.Name)];
}
