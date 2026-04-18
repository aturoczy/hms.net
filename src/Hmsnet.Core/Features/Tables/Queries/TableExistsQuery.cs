using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record TableExistsQuery(string DbName, string TableName)
    : IRequest<bool>, ICachedQuery
{
    public string CacheKey => $"table:exists:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTags.Table(DbName, TableName), CacheTags.TableList(DbName)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
