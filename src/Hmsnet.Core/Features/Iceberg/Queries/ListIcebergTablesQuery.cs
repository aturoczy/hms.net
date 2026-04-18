using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Queries;

public record ListIcebergTablesQuery(string DbName)
    : IRequest<IReadOnlyList<HiveTable>>, ICachedQuery
{
    public string CacheKey => $"iceberg:tables:{DbName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags => [CacheTags.TableList(DbName)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
