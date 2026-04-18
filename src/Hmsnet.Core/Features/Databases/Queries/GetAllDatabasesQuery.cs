using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Queries;

public record GetAllDatabasesQuery() : IRequest<IReadOnlyList<HiveDatabase>>, ICachedQuery
{
    public string CacheKey => "db:all";
    public IReadOnlyCollection<string> Tags => [CacheTags.DatabaseList];
    public TimeSpan Ttl => CacheTtl.Long;
}
