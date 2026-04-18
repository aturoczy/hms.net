using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetTableNamesLikeQuery(string DbName, string Pattern)
    : IRequest<IReadOnlyList<string>>, ICachedQuery
{
    public string CacheKey =>
        $"tables:like:{DbName.ToLowerInvariant()}:{Pattern}";
    public IReadOnlyCollection<string> Tags => [CacheTags.TableList(DbName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
