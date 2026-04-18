using Hmsnet.Core.Caching;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record RenameIcebergTableCommand(
    string FromDb,
    string FromTable,
    string ToDb,
    string ToTable)
    : IRequest, IInvalidatingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTags.TableList(FromDb),
        CacheTags.TableList(ToDb),
        CacheTags.Table(FromDb, FromTable),
        CacheTags.Table(ToDb, ToTable),
        CacheTags.Iceberg(FromDb, FromTable),
        CacheTags.Iceberg(ToDb, ToTable),
        CacheTags.Partitions(FromDb, FromTable),
        CacheTags.Partitions(ToDb, ToTable),
        CacheTags.Stats(FromDb, FromTable),
        CacheTags.Stats(ToDb, ToTable),
    ];
}
