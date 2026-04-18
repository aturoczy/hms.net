using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Queries;

public record GetAllDatabaseNamesQuery() : IRequest<IReadOnlyList<string>>, ICachedQuery
{
    public string CacheKey => "db:names";
    public IReadOnlyCollection<string> Tags => [CacheTags.DatabaseList];
    public TimeSpan Ttl => CacheTtl.Long;
}
