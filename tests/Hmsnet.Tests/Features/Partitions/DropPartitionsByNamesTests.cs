using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Features.Partitions;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;

namespace Hmsnet.Tests.Features.Partitions;

[TestClass]
public class DropPartitionsByNamesTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private static async Task<HiveTable> SeedPartitionedTableAsync()
    {
        var ctx = DbContextFactory.Create("drop-by-names-" + Guid.NewGuid());
        var (db, table) = await SeedData.SeedTableAsync(ctx, "sales", "orders",
            extraPartKeys: [new HiveColumn { Name = "dt", TypeName = "string", OrdinalPosition = 2, IsPartitionKey = true }]);

        var svc = new PartitionService(ctx);
        await svc.AddPartitionAsync("sales", "orders", SeedData.Partition(table.Id, ["2026-01-01"]));
        await svc.AddPartitionAsync("sales", "orders", SeedData.Partition(table.Id, ["2026-01-02"]));
        await svc.AddPartitionAsync("sales", "orders", SeedData.Partition(table.Id, ["2026-01-03"]));

        // return a *fresh* table object bound to a fresh context so subsequent
        // service calls exercise a clean state
        return table;
    }

    [TestMethod]
    public async Task DropPartitionsByNames_RemovesRequestedPartitions()
    {
        var dbName = "drop-by-names-" + Guid.NewGuid();
        await using var ctx = DbContextFactory.Create(dbName);
        var (_, table) = await SeedData.SeedTableAsync(ctx, "sales", "orders",
            extraPartKeys: [new HiveColumn { Name = "dt", TypeName = "string", OrdinalPosition = 2, IsPartitionKey = true }]);
        var svc = new PartitionService(ctx);
        await svc.AddPartitionAsync("sales", "orders", SeedData.Partition(table.Id, ["2026-01-01"]));
        await svc.AddPartitionAsync("sales", "orders", SeedData.Partition(table.Id, ["2026-01-02"]));
        await svc.AddPartitionAsync("sales", "orders", SeedData.Partition(table.Id, ["2026-01-03"]));

        var dropped = await new DropPartitionsByNamesHandler(svc).Handle(
            new DropPartitionsByNamesCommand("sales", "orders",
                ["dt=2026-01-01", "dt=2026-01-03"], DeleteData: false), CT);

        Assert.AreEqual(2, dropped);
        Assert.AreEqual(1, await svc.GetPartitionCountAsync("sales", "orders", CT));
    }

    [TestMethod]
    public async Task DropPartitionsByNames_IgnoresUnknownNames()
    {
        var dbName = "drop-by-names-" + Guid.NewGuid();
        await using var ctx = DbContextFactory.Create(dbName);
        var (_, table) = await SeedData.SeedTableAsync(ctx, "sales", "orders",
            extraPartKeys: [new HiveColumn { Name = "dt", TypeName = "string", OrdinalPosition = 2, IsPartitionKey = true }]);
        var svc = new PartitionService(ctx);
        await svc.AddPartitionAsync("sales", "orders", SeedData.Partition(table.Id, ["2026-01-01"]));

        var dropped = await new DropPartitionsByNamesHandler(svc).Handle(
            new DropPartitionsByNamesCommand("sales", "orders",
                ["dt=1999-12-31", "dt=2026-01-01"], DeleteData: false), CT);

        Assert.AreEqual(1, dropped);
        Assert.AreEqual(0, await svc.GetPartitionCountAsync("sales", "orders", CT));
    }
}
