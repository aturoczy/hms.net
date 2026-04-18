using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetAllTablesQuery(string DbName)
    : IRequest<IReadOnlyList<HiveTable>>, ICachedQuery
{
    public string CacheKey => $"tables:all:{DbName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags => [CacheTags.TableList(DbName)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
