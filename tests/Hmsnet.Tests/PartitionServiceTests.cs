using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests;

[TestClass]
public class PartitionServiceTests
{
    private static Task<(HiveDatabase db, HiveTable table)> SeedPartitionedTableAsync(
        Hmsnet.Infrastructure.Data.MetastoreDbContext ctx,
        string dbName = "db1", string tableName = "partitioned")
        => SeedData.SeedTableAsync(ctx, dbName, tableName,
               extraPartKeys: SeedData.DefaultPartitionKeys()); // partition key: dt string

    // ── Add ───────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AddPartition_Succeeds_AndPersists()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);

        var partition = SeedData.Partition(table.Id, ["2024-01-01"]);
        var result = await svc.AddPartitionAsync("db1", "partitioned", partition);

        Assert.IsTrue(result.Id > 0);
        Assert.AreEqual(1, await svc.GetPartitionCountAsync("db1", "partitioned"));
    }

    [TestMethod]
    public async Task AddPartition_SetsDefaultLocation_WhenEmpty()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);

        var partition = SeedData.Partition(table.Id, ["2024-01-01"]);
        partition.StorageDescriptor.Location = string.Empty;
        var result = await svc.AddPartitionAsync("db1", "partitioned", partition);

        StringAssert.Contains(result.StorageDescriptor.Location, "2024-01-01");
    }

    [TestMethod]
    public async Task AddPartition_ThrowsAlreadyExists_ForDuplicate()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);

        var p1 = SeedData.Partition(table.Id, ["2024-01-01"]);
        await svc.AddPartitionAsync("db1", "partitioned", p1);

        var p2 = SeedData.Partition(table.Id, ["2024-01-01"]);
        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            svc.AddPartitionAsync("db1", "partitioned", p2));
    }

    [TestMethod]
    public async Task AddPartition_ThrowsInvalidOperation_WhenValueCountMismatch()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        await SeedPartitionedTableAsync(ctx); // 1 partition key

        var p = SeedData.Partition(0, ["2024", "01"]); // 2 values — wrong
        await AssertEx.ThrowsAsync<Hmsnet.Core.Exceptions.InvalidOperationException>(() =>
            svc.AddPartitionAsync("db1", "partitioned", p));
    }

    [TestMethod]
    public async Task AddPartition_ThrowsNoSuchObject_WhenTableMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "db1");

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            svc.AddPartitionAsync("db1", "ghost", SeedData.Partition(0, ["2024"])));
    }

    // ── Get ───────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetPartition_ByValues_ReturnsCorrectPartition()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);

        await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01-01"]));

        var p = await svc.GetPartitionAsync("db1", "partitioned", ["2024-01-01"]);

        Assert.IsNotNull(p);
        CollectionAssert.AreEqual(new[] { "2024-01-01" }, p.Values);
    }

    [TestMethod]
    public async Task GetPartition_ReturnsNull_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        await SeedPartitionedTableAsync(ctx);

        var p = await svc.GetPartitionAsync("db1", "partitioned", ["9999-99-99"]);

        Assert.IsNull(p);
    }

    [TestMethod]
    public async Task GetPartitionByName_ReturnsCorrectPartition()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-06-15"]));

        var p = await svc.GetPartitionByNameAsync("db1", "partitioned", "dt=2024-06-15");

        Assert.IsNotNull(p);
        CollectionAssert.AreEqual(new[] { "2024-06-15" }, p.Values);
    }

    // ── List ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetPartitions_ReturnsAll_WithDefaultMaxParts()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02", "2024-03" })
            await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        var partitions = await svc.GetPartitionsAsync("db1", "partitioned");

        Assert.AreEqual(3, partitions.Count);
    }

    [TestMethod]
    public async Task GetPartitions_RespectsMaxParts()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02", "2024-03" })
            await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        var partitions = await svc.GetPartitionsAsync("db1", "partitioned", maxParts: 2);

        Assert.AreEqual(2, partitions.Count);
    }

    [TestMethod]
    public async Task GetPartitionNames_ReturnsFormattedNames()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01-15"]));

        var names = await svc.GetPartitionNamesAsync("db1", "partitioned");

        Assert.AreEqual(1, names.Count);
        Assert.AreEqual("dt=2024-01-15", names[0]);
    }

    [TestMethod]
    public async Task GetPartitionsByFilter_Equality_FiltersCorrectly()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02", "2023-12" })
            await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        var results = await svc.GetPartitionsByFilterAsync("db1", "partitioned", "dt='2024-01'");

        Assert.AreEqual(1, results.Count);
        CollectionAssert.AreEqual(new[] { "2024-01" }, results[0].Values);
    }

    [TestMethod]
    public async Task GetPartitionsByFilter_GreaterThan_FiltersCorrectly()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02", "2024-03" })
            await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        var results = await svc.GetPartitionsByFilterAsync("db1", "partitioned", "dt>'2024-01'");

        Assert.AreEqual(2, results.Count);
    }

    // ── Count ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetPartitionCount_ReturnsCorrectCount()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02" })
            await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        Assert.AreEqual(2, await svc.GetPartitionCountAsync("db1", "partitioned"));
    }

    // ── Alter ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AlterPartition_UpdatesParameters()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01"]));

        var updated = SeedData.Partition(table.Id, ["2024-01"]);
        updated.Parameters["numRows"] = "1000000";
        await svc.AlterPartitionAsync("db1", "partitioned", updated);

        var p = await svc.GetPartitionAsync("db1", "partitioned", ["2024-01"]);
        Assert.AreEqual("1000000", p!.Parameters["numRows"]);
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task DropPartition_ByValues_ReturnsTrue_AndRemoves()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01"]));

        var dropped = await svc.DropPartitionAsync("db1", "partitioned", ["2024-01"], deleteData: false);

        Assert.IsTrue(dropped);
        Assert.AreEqual(0, await svc.GetPartitionCountAsync("db1", "partitioned"));
    }

    [TestMethod]
    public async Task DropPartition_ReturnsFalse_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        await SeedPartitionedTableAsync(ctx);

        var dropped = await svc.DropPartitionAsync("db1", "partitioned", ["9999-99"], deleteData: false);

        Assert.IsFalse(dropped);
    }

    [TestMethod]
    public async Task DropPartitionByName_Succeeds()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await svc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-06"]));

        var dropped = await svc.DropPartitionByNameAsync("db1", "partitioned", "dt=2024-06", deleteData: false);

        Assert.IsTrue(dropped);
    }

    // ── Batch add ─────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AddPartitions_Batch_AddsAll()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);

        var partitions = new[]
        {
            SeedData.Partition(table.Id, ["2024-01"]),
            SeedData.Partition(table.Id, ["2024-02"]),
            SeedData.Partition(table.Id, ["2024-03"]),
        };

        var results = await svc.AddPartitionsAsync("db1", "partitioned", partitions);

        Assert.AreEqual(3, results.Count);
        Assert.AreEqual(3, await svc.GetPartitionCountAsync("db1", "partitioned"));
    }
}
