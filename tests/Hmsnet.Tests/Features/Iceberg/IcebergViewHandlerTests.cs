using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Iceberg.Views;
using Hmsnet.Infrastructure.Features.Iceberg.Views;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;

namespace Hmsnet.Tests.Features.Iceberg;

[TestClass]
public class IcebergViewHandlerTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private const string SampleMetadata =
        "{\"view-uuid\":\"abc\",\"format-version\":1,\"current-version-id\":1}";

    [TestMethod]
    public async Task CreateView_ThenLoad_RoundTrips()
    {
        await using var ctx = DbContextFactory.Create();
        await SeedData.SeedDatabaseAsync(ctx, "analytics");
        var svc = new IcebergViewService(ctx);

        var created = await new CreateIcebergViewHandler(svc).Handle(
            new CreateIcebergViewCommand("analytics", "daily_orders",
                "s3://warehouse/analytics/daily_orders/metadata/v1.metadata.json",
                SampleMetadata, CurrentVersionId: 1), CT);

        Assert.AreEqual("daily_orders", created.Name);
        Assert.AreEqual(1, created.CurrentVersionId);

        var loaded = await new LoadIcebergViewHandler(svc).Handle(
            new LoadIcebergViewQuery("analytics", "daily_orders"), CT);

        Assert.IsNotNull(loaded);
        Assert.AreEqual(created.Id, loaded.Id);
        Assert.AreEqual(SampleMetadata, loaded.MetadataJson);
    }

    [TestMethod]
    public async Task CreateView_Duplicate_Throws()
    {
        await using var ctx = DbContextFactory.Create();
        await SeedData.SeedDatabaseAsync(ctx, "analytics");
        var svc = new IcebergViewService(ctx);
        var handler = new CreateIcebergViewHandler(svc);

        await handler.Handle(new CreateIcebergViewCommand("analytics", "v", "loc", SampleMetadata, 1), CT);

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            handler.Handle(new CreateIcebergViewCommand("analytics", "v", "loc", SampleMetadata, 1), CT));
    }

    [TestMethod]
    public async Task ReplaceView_BumpsVersionAndMetadata()
    {
        await using var ctx = DbContextFactory.Create();
        await SeedData.SeedDatabaseAsync(ctx, "analytics");
        var svc = new IcebergViewService(ctx);

        await new CreateIcebergViewHandler(svc).Handle(
            new CreateIcebergViewCommand("analytics", "v", "loc-v1", SampleMetadata, 1), CT);

        var replaced = await new ReplaceIcebergViewHandler(svc).Handle(
            new ReplaceIcebergViewCommand("analytics", "v", "loc-v2",
                "{\"current-version-id\":2}", NewVersionId: 2), CT);

        Assert.AreEqual(2, replaced.CurrentVersionId);
        Assert.AreEqual("loc-v2", replaced.MetadataLocation);
    }

    [TestMethod]
    public async Task DropView_ThenLoad_ReturnsNull()
    {
        await using var ctx = DbContextFactory.Create();
        await SeedData.SeedDatabaseAsync(ctx, "analytics");
        var svc = new IcebergViewService(ctx);
        await new CreateIcebergViewHandler(svc).Handle(
            new CreateIcebergViewCommand("analytics", "v", "loc", SampleMetadata, 1), CT);

        await new DropIcebergViewHandler(svc).Handle(new DropIcebergViewCommand("analytics", "v"), CT);

        var loaded = await svc.LoadViewAsync("analytics", "v", CT);
        Assert.IsNull(loaded);
    }

    [TestMethod]
    public async Task RenameView_MovesAcrossNamespaces()
    {
        await using var ctx = DbContextFactory.Create();
        await SeedData.SeedDatabaseAsync(ctx, "src");
        await SeedData.SeedDatabaseAsync(ctx, "dest");
        var svc = new IcebergViewService(ctx);
        await new CreateIcebergViewHandler(svc).Handle(
            new CreateIcebergViewCommand("src", "v", "loc", SampleMetadata, 1), CT);

        await new RenameIcebergViewHandler(svc).Handle(
            new RenameIcebergViewCommand("src", "v", "dest", "renamed_v"), CT);

        Assert.IsNull(await svc.LoadViewAsync("src", "v", CT));
        Assert.IsNotNull(await svc.LoadViewAsync("dest", "renamed_v", CT));
    }

    [TestMethod]
    public async Task RenameView_TargetExists_Throws()
    {
        await using var ctx = DbContextFactory.Create();
        await SeedData.SeedDatabaseAsync(ctx, "ns");
        var svc = new IcebergViewService(ctx);
        await new CreateIcebergViewHandler(svc).Handle(
            new CreateIcebergViewCommand("ns", "a", "loc", SampleMetadata, 1), CT);
        await new CreateIcebergViewHandler(svc).Handle(
            new CreateIcebergViewCommand("ns", "b", "loc", SampleMetadata, 1), CT);

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            new RenameIcebergViewHandler(svc).Handle(
                new RenameIcebergViewCommand("ns", "a", "ns", "b"), CT));
    }
}
