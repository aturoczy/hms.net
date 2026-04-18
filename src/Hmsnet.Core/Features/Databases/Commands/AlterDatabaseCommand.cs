using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record AlterDatabaseCommand(string Name, HiveDatabase Updated)
    : IRequest<HiveDatabase>, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTags.DatabaseList, CacheTags.Database(Name)];
}
