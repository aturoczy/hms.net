using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.ColumnStatistics.Commands;
using Hmsnet.Core.Features.ColumnStatistics.Queries;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Features.ColumnStatistics;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Features.ColumnStatistics;

[TestClass]
public class ColumnStatisticsHandlerTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private static Core.Models.ColumnStatistics LongStat(string columnName) => new()
    {
        ColumnName = columnName,
        ColumnType = "bigint",
        StatisticsType = StatisticsType.Long,
        NumNulls = 0,
        NumDistinctValues = 100,
        LongLow = 1,
        LongHigh = 1000
    };

    private static Core.Models.ColumnStatistics StringStat(string columnName) => new()
    {
        ColumnName = columnName,
        ColumnType = "string",
        StatisticsType = StatisticsType.String,
        NumNulls = 5,
        NumDistinctValues = 42,
        MaxColLen = 255,
        AvgColLen = 12.5
    };

    // ── Table statistics ──────────────────────────────────────────────────────

    [TestMethod]
    public async Task UpdateTableColumnStatistics_Inserts_WhenNew()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");

        await new UpdateTableStatsHandler(svc).Handle(
            new UpdateTableStatsCommand("db1", "t1", [LongStat("id")]), CT);

        var stats = await new GetTableStatsHandler(svc).Handle(
            new GetTableStatsQuery("db1", "t1", ["id"]), CT);

        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual("id", stats[0].ColumnName);
        Assert.AreEqual(1L, stats[0].LongLow);
        Assert.AreEqual(1000L, stats[0].LongHigh);
    }

    [TestMethod]
    public async Task UpdateTableColumnStatistics_Upserts_WhenAlreadyExists()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");
        var updateHandler = new UpdateTableStatsHandler(svc);

        await updateHandler.Handle(new UpdateTableStatsCommand("db1", "t1", [LongStat("id")]), CT);

        var updated = LongStat("id");
        updated.LongHigh = 9999;
        await updateHandler.Handle(new UpdateTableStatsCommand("db1", "t1", [updated]), CT);

        var stats = await new GetTableStatsHandler(svc).Handle(
            new GetTableStatsQuery("db1", "t1", ["id"]), CT);

        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual(9999L, stats[0].LongHigh);
    }

    [TestMethod]
    public async Task GetTableColumnStatistics_ReturnsOnlyRequestedColumns()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");

        await new UpdateTableStatsHandler(svc).Handle(
            new UpdateTableStatsCommand("db1", "t1", [LongStat("id"), StringStat("name")]), CT);

        var stats = await new GetTableStatsHandler(svc).Handle(
            new GetTableStatsQuery("db1", "t1", ["id"]), CT);

        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual("id", stats[0].ColumnName);
    }

    [TestMethod]
    public async Task GetTableColumnStatistics_ThrowsNoSuchObject_WhenTableMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "db1");

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new GetTableStatsHandler(svc).Handle(
                new GetTableStatsQuery("db1", "ghost", ["id"]), CT));
    }

    [TestMethod]
    public async Task DeleteTableColumnStatistics_RemovesSpecificColumn()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");
        await new UpdateTableStatsHandler(svc).Handle(
            new UpdateTableStatsCommand("db1", "t1", [LongStat("id"), StringStat("name")]), CT);

        await new DeleteTableStatsHandler(svc).Handle(
            new DeleteTableStatsCommand("db1", "t1", "id"), CT);

        var stats = await new GetTableStatsHandler(svc).Handle(
            new GetTableStatsQuery("db1", "t1", ["id", "name"]), CT);

        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual("name", stats[0].ColumnName);
    }

    [TestMethod]
    public async Task DeleteTableColumnStatistics_RemovesAll_WhenColumnNameIsNull()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");
        await new UpdateTableStatsHandler(svc).Handle(
            new UpdateTableStatsCommand("db1", "t1", [LongStat("id"), StringStat("name")]), CT);

        await new DeleteTableStatsHandler(svc).Handle(
            new DeleteTableStatsCommand("db1", "t1", null), CT);

        var stats = await new GetTableStatsHandler(svc).Handle(
            new GetTableStatsQuery("db1", "t1", ["id", "name"]), CT);

        Assert.AreEqual(0, stats.Count);
    }

    // ── Partition statistics ──────────────────────────────────────────────────

    [TestMethod]
    public async Task UpdatePartitionColumnStatistics_Inserts_WhenNew()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var statsSvc = new ColumnStatisticsService(ctx);
        var (_, table) = await SeedData.SeedTableAsync(ctx, "db1", "t1",
            extraPartKeys: SeedData.DefaultPartitionKeys());

        await partSvc.AddPartitionAsync("db1", "t1", SeedData.Partition(table.Id, ["2024-01"]));

        await new UpdatePartitionStatsHandler(statsSvc).Handle(
            new UpdatePartitionStatsCommand("db1", "t1", ["2024-01"], [LongStat("id")]), CT);

        var stats = await new GetPartitionStatsHandler(statsSvc).Handle(
            new GetPartitionStatsQuery("db1", "t1", ["2024-01"], ["id"]), CT);

        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual("id", stats[0].ColumnName);
    }

    [TestMethod]
    public async Task DeletePartitionColumnStatistics_RemovesAll_WhenColumnIsNull()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var statsSvc = new ColumnStatisticsService(ctx);
        var (_, table) = await SeedData.SeedTableAsync(ctx, "db1", "t1",
            extraPartKeys: SeedData.DefaultPartitionKeys());
        await partSvc.AddPartitionAsync("db1", "t1", SeedData.Partition(table.Id, ["2024-01"]));
        await new UpdatePartitionStatsHandler(statsSvc).Handle(
            new UpdatePartitionStatsCommand("db1", "t1", ["2024-01"], [LongStat("id"), StringStat("name")]), CT);

        await new DeletePartitionStatsHandler(statsSvc).Handle(
            new DeletePartitionStatsCommand("db1", "t1", ["2024-01"], null), CT);

        var stats = await new GetPartitionStatsHandler(statsSvc).Handle(
            new GetPartitionStatsQuery("db1", "t1", ["2024-01"], ["id", "name"]), CT);

        Assert.AreEqual(0, stats.Count);
    }

    [TestMethod]
    public async Task GetPartitionColumnStatistics_ThrowsNoSuchObject_WhenPartitionMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1",
            extraPartKeys: SeedData.DefaultPartitionKeys());

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new GetPartitionStatsHandler(svc).Handle(
                new GetPartitionStatsQuery("db1", "t1", ["9999-99"], ["id"]), CT));
    }
}
