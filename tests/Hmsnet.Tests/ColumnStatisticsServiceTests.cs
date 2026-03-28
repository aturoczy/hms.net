using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests;

[TestClass]
public class ColumnStatisticsServiceTests
{
    private static ColumnStatistics LongStat(string columnName) => new()
    {
        ColumnName = columnName,
        ColumnType = "bigint",
        StatisticsType = StatisticsType.Long,
        NumNulls = 0,
        NumDistinctValues = 100,
        LongLow = 1,
        LongHigh = 1000
    };

    private static ColumnStatistics StringStat(string columnName) => new()
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

        await svc.UpdateTableColumnStatisticsAsync("db1", "t1", [LongStat("id")]);

        var stats = await svc.GetTableColumnStatisticsAsync("db1", "t1", ["id"]);
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

        await svc.UpdateTableColumnStatisticsAsync("db1", "t1", [LongStat("id")]);

        var updated = LongStat("id");
        updated.LongHigh = 9999;
        await svc.UpdateTableColumnStatisticsAsync("db1", "t1", [updated]);

        var stats = await svc.GetTableColumnStatisticsAsync("db1", "t1", ["id"]);
        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual(9999L, stats[0].LongHigh);
    }

    [TestMethod]
    public async Task GetTableColumnStatistics_ReturnsOnlyRequestedColumns()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");

        await svc.UpdateTableColumnStatisticsAsync("db1", "t1",
            [LongStat("id"), StringStat("name")]);

        var stats = await svc.GetTableColumnStatisticsAsync("db1", "t1", ["id"]);

        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual("id", stats[0].ColumnName);
    }

    [TestMethod]
    public async Task GetTableColumnStatistics_ReturnsEmpty_WhenNoneExist()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");

        var stats = await svc.GetTableColumnStatisticsAsync("db1", "t1", ["id"]);

        Assert.AreEqual(0, stats.Count);
    }

    [TestMethod]
    public async Task GetTableColumnStatistics_ThrowsNoSuchObject_WhenTableMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "db1");

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            svc.GetTableColumnStatisticsAsync("db1", "ghost", ["id"]));
    }

    [TestMethod]
    public async Task DeleteTableColumnStatistics_RemovesSpecificColumn()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");
        await svc.UpdateTableColumnStatisticsAsync("db1", "t1",
            [LongStat("id"), StringStat("name")]);

        await svc.DeleteTableColumnStatisticsAsync("db1", "t1", "id");

        var stats = await svc.GetTableColumnStatisticsAsync("db1", "t1", ["id", "name"]);
        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual("name", stats[0].ColumnName);
    }

    [TestMethod]
    public async Task DeleteTableColumnStatistics_RemovesAll_WhenColumnNameIsNull()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");
        await svc.UpdateTableColumnStatisticsAsync("db1", "t1",
            [LongStat("id"), StringStat("name")]);

        await svc.DeleteTableColumnStatisticsAsync("db1", "t1", null);

        var stats = await svc.GetTableColumnStatisticsAsync("db1", "t1", ["id", "name"]);
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

        await statsSvc.UpdatePartitionColumnStatisticsAsync(
            "db1", "t1", ["2024-01"], [LongStat("id")]);

        var stats = await statsSvc.GetPartitionColumnStatisticsAsync(
            "db1", "t1", ["2024-01"], ["id"]);

        Assert.AreEqual(1, stats.Count);
        Assert.AreEqual("id", stats[0].ColumnName);
    }

    [TestMethod]
    public async Task UpdatePartitionColumnStatistics_Upserts_WhenAlreadyExists()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var statsSvc = new ColumnStatisticsService(ctx);
        var (_, table) = await SeedData.SeedTableAsync(ctx, "db1", "t1",
            extraPartKeys: SeedData.DefaultPartitionKeys());
        await partSvc.AddPartitionAsync("db1", "t1", SeedData.Partition(table.Id, ["2024-01"]));
        await statsSvc.UpdatePartitionColumnStatisticsAsync("db1", "t1", ["2024-01"], [LongStat("id")]);

        var updated = LongStat("id");
        updated.LongLow = 42;
        await statsSvc.UpdatePartitionColumnStatisticsAsync("db1", "t1", ["2024-01"], [updated]);

        var stats = await statsSvc.GetPartitionColumnStatisticsAsync("db1", "t1", ["2024-01"], ["id"]);
        Assert.AreEqual(42L, stats[0].LongLow);
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
        await statsSvc.UpdatePartitionColumnStatisticsAsync("db1", "t1", ["2024-01"],
            [LongStat("id"), StringStat("name")]);

        await statsSvc.DeletePartitionColumnStatisticsAsync("db1", "t1", ["2024-01"], null);

        var stats = await statsSvc.GetPartitionColumnStatisticsAsync(
            "db1", "t1", ["2024-01"], ["id", "name"]);
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
            svc.GetPartitionColumnStatisticsAsync("db1", "t1", ["9999-99"], ["id"]));
    }

    // ── Statistics content correctness ────────────────────────────────────────

    [TestMethod]
    public async Task UpdateTableColumnStatistics_StoresStringStats_Correctly()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new ColumnStatisticsService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1");
        var stat = StringStat("name");

        await svc.UpdateTableColumnStatisticsAsync("db1", "t1", [stat]);

        var stats = await svc.GetTableColumnStatisticsAsync("db1", "t1", ["name"]);
        Assert.AreEqual(StatisticsType.String, stats[0].StatisticsType);
        Assert.AreEqual(5L, stats[0].NumNulls);
        Assert.AreEqual(255L, stats[0].MaxColLen);
        Assert.AreEqual(12.5, stats[0].AvgColLen);
    }
}
