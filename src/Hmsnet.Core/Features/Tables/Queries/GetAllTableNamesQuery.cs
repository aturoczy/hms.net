using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetAllTableNamesQuery(string DbName)
    : IRequest<IReadOnlyList<string>>, ICachedQuery
{
    public string CacheKey => $"tables:names:{DbName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags => [CacheTags.TableList(DbName)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
