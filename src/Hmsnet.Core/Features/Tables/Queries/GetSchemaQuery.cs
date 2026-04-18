using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetSchemaQuery(string DbName, string TableName)
    : IRequest<IReadOnlyList<HiveColumn>>, ICachedQuery
{
    public string CacheKey => $"table:schema:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags => [CacheTags.Table(DbName, TableName)];
    public TimeSpan Ttl => CacheTtl.Medium;
}
