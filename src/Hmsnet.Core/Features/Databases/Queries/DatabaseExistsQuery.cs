using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Queries;

public record DatabaseExistsQuery(string Name) : IRequest<bool>, ICachedQuery
{
    public string CacheKey => $"db:exists:{Name.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTags.Database(Name), CacheTags.DatabaseList];
    public TimeSpan Ttl => CacheTtl.Medium;
}
