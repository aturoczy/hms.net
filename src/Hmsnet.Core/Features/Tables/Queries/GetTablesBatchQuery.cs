using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetTablesBatchQuery(string DbName, List<string> TableNames)
    : IRequest<IReadOnlyList<HiveTable>>, ICachedQuery
{
    public string CacheKey =>
        $"tables:batch:{DbName.ToLowerInvariant()}:{string.Join(',', TableNames.Select(n => n.ToLowerInvariant()).OrderBy(n => n))}";
    public IReadOnlyCollection<string> Tags => [CacheTags.TableList(DbName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
