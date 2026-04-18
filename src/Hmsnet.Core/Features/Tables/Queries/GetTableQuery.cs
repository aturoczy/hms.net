using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetTableQuery(string DbName, string TableName)
    : IRequest<HiveTable?>, ICachedQuery
{
    public string CacheKey => $"table:get:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTags.Table(DbName, TableName), CacheTags.TableList(DbName)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
