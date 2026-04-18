using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.ColumnStatistics.Commands;

public record DeleteTableStatsCommand(string DbName, string TableName, string? Column)
    : IRequest, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags => [CacheTags.Stats(DbName, TableName)];
}
