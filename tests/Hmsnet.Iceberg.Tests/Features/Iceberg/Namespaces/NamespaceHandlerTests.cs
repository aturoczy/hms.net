using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Features.Iceberg.Queries;
using Hmsnet.Iceberg.Tests.Helpers;
using Hmsnet.Infrastructure.Features.Iceberg.Namespaces;
using Hmsnet.Infrastructure.Services;

namespace Hmsnet.Iceberg.Tests.Features.Iceberg.Namespaces;

[TestClass]
public class NamespaceHandlerTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private static (IcebergCatalogService svc, Infrastructure.Data.MetastoreDbContext ctx) CreateSvc(
        Infrastructure.Data.MetastoreDbContext ctx)
    {
        var dbSvc = new DatabaseService(ctx);
        var tableSvc = new TableService(ctx);
        return (new IcebergCatalogService(dbSvc, tableSvc, ctx), ctx);
    }

    // ── Create ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task CreateNamespace_Succeeds_AndReturnsDatabase()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);

        var db = await new CreateIcebergNamespaceHandler(svc).Handle(
            new CreateIcebergNamespaceCommand("catalog1", []), CT);

        Assert.IsNotNull(db);
        Assert.AreEqual("catalog1", db.Name);
        Assert.IsTrue(db.Id > 0);
    }

    [TestMethod]
    public async Task CreateNamespace_WithProperties_StoresProperties()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);
        var props = new Dictionary<string, string> { ["owner"] = "alice", ["env"] = "prod" };

        var db = await new CreateIcebergNamespaceHandler(svc).Handle(
            new CreateIcebergNamespaceCommand("propdb", props), CT);

        Assert.AreEqual("alice", db.Parameters["owner"]);
        Assert.AreEqual("prod", db.Parameters["env"]);
    }

    [TestMethod]
    public async Task CreateNamespace_Duplicate_ThrowsAlreadyExistsException()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);
        var handler = new CreateIcebergNamespaceHandler(svc);
        await handler.Handle(new CreateIcebergNamespaceCommand("dup", []), CT);

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            handler.Handle(new CreateIcebergNamespaceCommand("dup", []), CT));
    }

    // ── Get ───────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetNamespace_ReturnsCorrectNamespace()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);
        await new CreateIcebergNamespaceHandler(svc).Handle(
            new CreateIcebergNamespaceCommand("myns", new Dictionary<string, string> { ["k"] = "v" }), CT);

        var db = await new GetIcebergNamespaceHandler(svc).Handle(new GetIcebergNamespaceQuery("myns"), CT);

        Assert.IsNotNull(db);
        Assert.AreEqual("myns", db.Name);
        Assert.AreEqual("v", db.Parameters["k"]);
    }

    [TestMethod]
    public async Task GetNamespace_ReturnsNull_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);

        var db = await new GetIcebergNamespaceHandler(svc).Handle(
            new GetIcebergNamespaceQuery("notexist"), CT);

        Assert.IsNull(db);
    }

    // ── List ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task ListNamespaces_ReturnsAll()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);
        var handler = new CreateIcebergNamespaceHandler(svc);
        await handler.Handle(new CreateIcebergNamespaceCommand("ns1", []), CT);
        await handler.Handle(new CreateIcebergNamespaceCommand("ns2", []), CT);
        await handler.Handle(new CreateIcebergNamespaceCommand("ns3", []), CT);

        var namespaces = await new ListIcebergNamespacesHandler(svc).Handle(
            new ListIcebergNamespacesQuery(), CT);

        Assert.AreEqual(3, namespaces.Count);
        CollectionAssert.IsSubsetOf(
            new[] { "ns1", "ns2", "ns3" },
            namespaces.Select(n => n.Name).ToList());
    }

    // ── Update Properties ─────────────────────────────────────────────────────

    [TestMethod]
    public async Task UpdateNamespaceProperties_AddsAndRemovesKeys()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);
        await new CreateIcebergNamespaceHandler(svc).Handle(
            new CreateIcebergNamespaceCommand("upd", new Dictionary<string, string>
            {
                ["existing"] = "val",
                ["to_remove"] = "bye"
            }), CT);

        var (updated, removed) = await new UpdateIcebergNamespacePropertiesHandler(svc).Handle(
            new UpdateIcebergNamespacePropertiesCommand(
                "upd",
                ["to_remove"],
                new Dictionary<string, string> { ["new_key"] = "new_val" }), CT);

        Assert.IsTrue(updated.Contains("new_key"));
        Assert.IsTrue(removed.Contains("to_remove"));

        var ns = await new GetIcebergNamespaceHandler(svc).Handle(new GetIcebergNamespaceQuery("upd"), CT);
        Assert.IsNotNull(ns);
        Assert.IsTrue(ns.Parameters.ContainsKey("new_key"));
        Assert.IsFalse(ns.Parameters.ContainsKey("to_remove"));
        Assert.AreEqual("val", ns.Parameters["existing"]);
    }

    [TestMethod]
    public async Task UpdateNamespaceProperties_ThrowsNoSuchObject_WhenNamespaceNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new UpdateIcebergNamespacePropertiesHandler(svc).Handle(
                new UpdateIcebergNamespacePropertiesCommand("ghost", [], []), CT));
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task DropNamespace_Succeeds_WhenEmpty()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);
        await new CreateIcebergNamespaceHandler(svc).Handle(
            new CreateIcebergNamespaceCommand("todrop", []), CT);

        await new DropIcebergNamespaceHandler(svc).Handle(
            new DropIcebergNamespaceCommand("todrop"), CT);

        var ns = await new GetIcebergNamespaceHandler(svc).Handle(
            new GetIcebergNamespaceQuery("todrop"), CT);
        Assert.IsNull(ns);
    }

    [TestMethod]
    public async Task DropNamespace_ThrowsNoSuchObject_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var (svc, _) = CreateSvc(ctx);

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new DropIcebergNamespaceHandler(svc).Handle(
                new DropIcebergNamespaceCommand("notexist"), CT));
    }
}
