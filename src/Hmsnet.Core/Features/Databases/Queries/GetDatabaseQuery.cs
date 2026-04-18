using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Queries;

public record GetDatabaseQuery(string Name) : IRequest<HiveDatabase?>, ICachedQuery
{
    public string CacheKey => $"db:get:{Name.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTags.Database(Name), CacheTags.DatabaseList];
    public TimeSpan Ttl => CacheTtl.Medium;
}
