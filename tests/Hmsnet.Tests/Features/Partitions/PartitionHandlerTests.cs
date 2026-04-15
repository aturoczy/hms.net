using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Infrastructure.Features.Partitions;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Features.Partitions;

[TestClass]
public class PartitionHandlerTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private static Task<(Hmsnet.Core.Models.HiveDatabase db, Hmsnet.Core.Models.HiveTable table)>
        SeedPartitionedTableAsync(Hmsnet.Infrastructure.Data.MetastoreDbContext ctx,
            string dbName = "db1", string tableName = "partitioned")
        => SeedData.SeedTableAsync(ctx, dbName, tableName,
               extraPartKeys: SeedData.DefaultPartitionKeys());

    // ── Add ───────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AddPartition_Succeeds_AndPersists()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);

        var result = await new AddPartitionHandler(partSvc, tableSvc).Handle(
            new AddPartitionCommand("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01-01"])), CT);

        Assert.IsTrue(result.Partition.Id > 0);
        Assert.AreEqual(1, await new GetPartitionCountHandler(partSvc).Handle(
            new GetPartitionCountQuery("db1", "partitioned"), CT));
    }

    [TestMethod]
    public async Task AddPartition_ThrowsAlreadyExists_ForDuplicate()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        var addHandler = new AddPartitionHandler(partSvc, tableSvc);

        await addHandler.Handle(
            new AddPartitionCommand("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01-01"])), CT);

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            addHandler.Handle(
                new AddPartitionCommand("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01-01"])), CT));
    }

    [TestMethod]
    public async Task AddPartition_ThrowsInvalidOperation_WhenValueCountMismatch()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        await SeedPartitionedTableAsync(ctx);

        await AssertEx.ThrowsAsync<Hmsnet.Core.Exceptions.InvalidOperationException>(() =>
            new AddPartitionHandler(partSvc, tableSvc).Handle(
                new AddPartitionCommand("db1", "partitioned", SeedData.Partition(0, ["2024", "01"])), CT));
    }

    [TestMethod]
    public async Task AddPartition_ThrowsNoSuchObject_WhenTableMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "db1");

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new AddPartitionHandler(partSvc, tableSvc).Handle(
                new AddPartitionCommand("db1", "ghost", SeedData.Partition(0, ["2024"])), CT));
    }

    // ── Get ───────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetPartitionByValues_ReturnsCorrectPartition()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01-01"]));

        var result = await new GetPartitionByValuesHandler(partSvc, tableSvc).Handle(
            new GetPartitionByValuesQuery("db1", "partitioned", ["2024-01-01"]), CT);

        Assert.IsNotNull(result);
        CollectionAssert.AreEqual(new[] { "2024-01-01" }, result.Partition.Values);
    }

    [TestMethod]
    public async Task GetPartitionByValues_ReturnsNull_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        await SeedPartitionedTableAsync(ctx);

        var result = await new GetPartitionByValuesHandler(partSvc, tableSvc).Handle(
            new GetPartitionByValuesQuery("db1", "partitioned", ["9999-99-99"]), CT);

        Assert.IsNull(result);
    }

    [TestMethod]
    public async Task GetPartitionByName_ReturnsCorrectPartition()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-06-15"]));

        var result = await new GetPartitionByNameHandler(partSvc, tableSvc).Handle(
            new GetPartitionByNameQuery("db1", "partitioned", "dt=2024-06-15"), CT);

        Assert.IsNotNull(result);
        CollectionAssert.AreEqual(new[] { "2024-06-15" }, result.Partition.Values);
    }

    // ── List ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetPartitions_ReturnsAll_WithDefaultMaxParts()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02", "2024-03" })
            await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        var result = await new GetPartitionsHandler(partSvc, tableSvc).Handle(
            new GetPartitionsQuery("db1", "partitioned"), CT);

        Assert.AreEqual(3, result.Partitions.Count);
        Assert.IsTrue(result.PartitionKeys.Any());
    }

    [TestMethod]
    public async Task GetPartitions_RespectsMaxParts()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02", "2024-03" })
            await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        var result = await new GetPartitionsHandler(partSvc, tableSvc).Handle(
            new GetPartitionsQuery("db1", "partitioned", MaxParts: 2), CT);

        Assert.AreEqual(2, result.Partitions.Count);
    }

    [TestMethod]
    public async Task GetPartitionNames_ReturnsFormattedNames()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01-15"]));

        var names = await new GetPartitionNamesHandler(partSvc).Handle(
            new GetPartitionNamesQuery("db1", "partitioned"), CT);

        Assert.AreEqual(1, names.Count);
        Assert.AreEqual("dt=2024-01-15", names[0]);
    }

    [TestMethod]
    public async Task GetPartitionsByFilter_Equality_FiltersCorrectly()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02", "2023-12" })
            await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        var result = await new GetPartitionsByFilterHandler(partSvc, tableSvc).Handle(
            new GetPartitionsByFilterQuery("db1", "partitioned", "dt='2024-01'"), CT);

        Assert.AreEqual(1, result.Partitions.Count);
        CollectionAssert.AreEqual(new[] { "2024-01" }, result.Partitions[0].Values);
    }

    // ── Count ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetPartitionCount_ReturnsCorrectCount()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        foreach (var d in new[] { "2024-01", "2024-02" })
            await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, [d]));

        Assert.AreEqual(2, await new GetPartitionCountHandler(partSvc).Handle(
            new GetPartitionCountQuery("db1", "partitioned"), CT));
    }

    // ── Alter ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AlterPartition_UpdatesParameters()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01"]));

        var updated = SeedData.Partition(table.Id, ["2024-01"]);
        updated.Parameters["numRows"] = "1000000";

        await new AlterPartitionHandler(partSvc, tableSvc).Handle(
            new AlterPartitionCommand("db1", "partitioned", updated), CT);

        var p = await partSvc.GetPartitionAsync("db1", "partitioned", ["2024-01"]);
        Assert.AreEqual("1000000", p!.Parameters["numRows"]);
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task DropPartition_ByValues_ReturnsTrue_AndRemoves()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-01"]));

        var dropped = await new DropPartitionHandler(partSvc).Handle(
            new DropPartitionCommand("db1", "partitioned", ["2024-01"], false), CT);

        Assert.IsTrue(dropped);
        Assert.AreEqual(0, await new GetPartitionCountHandler(partSvc).Handle(
            new GetPartitionCountQuery("db1", "partitioned"), CT));
    }

    [TestMethod]
    public async Task DropPartition_ReturnsFalse_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        await SeedPartitionedTableAsync(ctx);

        var dropped = await new DropPartitionHandler(partSvc).Handle(
            new DropPartitionCommand("db1", "partitioned", ["9999-99"], false), CT);

        Assert.IsFalse(dropped);
    }

    [TestMethod]
    public async Task DropPartitionByName_Succeeds()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);
        await partSvc.AddPartitionAsync("db1", "partitioned", SeedData.Partition(table.Id, ["2024-06"]));

        var dropped = await new DropPartitionByNameHandler(partSvc).Handle(
            new DropPartitionByNameCommand("db1", "partitioned", "dt=2024-06", false), CT);

        Assert.IsTrue(dropped);
    }

    // ── Batch add ─────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AddPartitions_Batch_AddsAll()
    {
        await using var ctx = DbContextFactory.Create();
        var partSvc = new PartitionService(ctx);
        var tableSvc = new TableService(ctx);
        var (_, table) = await SeedPartitionedTableAsync(ctx);

        var partitions = new List<Hmsnet.Core.Models.HivePartition>
        {
            SeedData.Partition(table.Id, ["2024-01"]),
            SeedData.Partition(table.Id, ["2024-02"]),
            SeedData.Partition(table.Id, ["2024-03"]),
        };

        var result = await new AddPartitionsHandler(partSvc, tableSvc).Handle(
            new AddPartitionsCommand("db1", "partitioned", partitions), CT);

        Assert.AreEqual(3, result.Partitions.Count);
        Assert.AreEqual(3, await new GetPartitionCountHandler(partSvc).Handle(
            new GetPartitionCountQuery("db1", "partitioned"), CT));
    }
}
