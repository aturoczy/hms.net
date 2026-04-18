using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Queries;

public record LoadIcebergTableQuery(string DbName, string TableName)
    : IRequest<IcebergTableMetadata?>, ICachedQuery
{
    public string CacheKey =>
        $"iceberg:load:{DbName.ToLowerInvariant()}:{TableName.ToLowerInvariant()}";
    public IReadOnlyCollection<string> Tags =>
        [CacheTags.Iceberg(DbName, TableName), CacheTags.Table(DbName, TableName)];
    public TimeSpan Ttl => CacheTtl.Short;
}
