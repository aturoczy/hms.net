using Hmsnet.Core.Caching;
using Hmsnet.Core.Models;
using Hmsnet.Core.Notifications;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Views;

public record CreateIcebergViewCommand(
    string DbName,
    string ViewName,
    string MetadataLocation,
    string MetadataJson,
    int CurrentVersionId)
    : IRequest<IcebergView>, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTagsExtra.IcebergViewList(DbName), CacheTagsExtra.IcebergView(DbName, ViewName)];

    string IEventEmittingCommand.EventType => "CREATE_VIEW";
    string? IEventEmittingCommand.DbName => DbName;
    string? IEventEmittingCommand.TableName => ViewName;
}

public record ReplaceIcebergViewCommand(
    string DbName,
    string ViewName,
    string NewMetadataLocation,
    string NewMetadataJson,
    int NewVersionId)
    : IRequest<IcebergView>, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTagsExtra.IcebergViewList(DbName), CacheTagsExtra.IcebergView(DbName, ViewName)];

    string IEventEmittingCommand.EventType => "REPLACE_VIEW";
    string? IEventEmittingCommand.DbName => DbName;
    string? IEventEmittingCommand.TableName => ViewName;
}

public record DropIcebergViewCommand(string DbName, string ViewName)
    : IRequest, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
        [CacheTagsExtra.IcebergViewList(DbName), CacheTagsExtra.IcebergView(DbName, ViewName)];

    string IEventEmittingCommand.EventType => "DROP_VIEW";
    string? IEventEmittingCommand.DbName => DbName;
    string? IEventEmittingCommand.TableName => ViewName;
}

public record RenameIcebergViewCommand(
    string FromDb, string FromView,
    string ToDb, string ToView)
    : IRequest, IInvalidatingCommand, IEventEmittingCommand
{
    public IReadOnlyCollection<string> InvalidatesTags =>
    [
        CacheTagsExtra.IcebergViewList(FromDb),
        CacheTagsExtra.IcebergViewList(ToDb),
        CacheTagsExtra.IcebergView(FromDb, FromView),
        CacheTagsExtra.IcebergView(ToDb, ToView),
    ];

    string IEventEmittingCommand.EventType => "RENAME_VIEW";
    string? IEventEmittingCommand.DbName => FromDb;
    string? IEventEmittingCommand.TableName => FromView;
    object? IEventEmittingCommand.EventPayload => new { ToDb, ToView };
}
