using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.ColumnStatistics.Commands;

public record UpdateTableStatsCommand(string DbName, string TableName, IEnumerable<Hmsnet.Core.Models.ColumnStatistics> Stats)
    : IRequest, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags => [CacheTags.Stats(DbName, TableName)];
}
