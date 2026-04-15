using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Databases.Commands;
using Hmsnet.Core.Features.Databases.Queries;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Features.Databases;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Features.Databases;

[TestClass]
public class DatabaseHandlerTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    // ── Create ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task CreateDatabase_Succeeds_AndAssignsId()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        var db = await new CreateDatabaseHandler(svc).Handle(
            new CreateDatabaseCommand(SeedData.Database("sales")), CT);

        Assert.IsGreaterThan(0, db.Id);
        Assert.AreEqual("sales", db.Name);
        Assert.IsGreaterThan(0, db.CreateTime);
    }

    [TestMethod]
    public async Task CreateDatabase_NormalizesNameToLowercase()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        var db = await new CreateDatabaseHandler(svc).Handle(
            new CreateDatabaseCommand(SeedData.Database("UPPERCASE_DB")), CT);

        Assert.AreEqual("uppercase_db", db.Name);
    }

    [TestMethod]
    public async Task CreateDatabase_ThrowsAlreadyExists_ForDuplicateName()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        var handler = new CreateDatabaseHandler(svc);
        await handler.Handle(new CreateDatabaseCommand(SeedData.Database("dup")), CT);

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            handler.Handle(new CreateDatabaseCommand(SeedData.Database("dup")), CT));
    }

    // ── Get / Exists ──────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetDatabase_ReturnsCorrectDatabase()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await new CreateDatabaseHandler(svc).Handle(
            new CreateDatabaseCommand(SeedData.Database("orders")), CT);

        var db = await new GetDatabaseHandler(svc).Handle(new GetDatabaseQuery("orders"), CT);

        Assert.IsNotNull(db);
        Assert.AreEqual("orders", db.Name);
    }

    [TestMethod]
    public async Task GetDatabase_ReturnsNull_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        var db = await new GetDatabaseHandler(svc).Handle(new GetDatabaseQuery("nonexistent"), CT);

        Assert.IsNull(db);
    }

    [TestMethod]
    public async Task DatabaseExists_ReturnsTrue_WhenPresent()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await new CreateDatabaseHandler(svc).Handle(
            new CreateDatabaseCommand(SeedData.Database("present")), CT);

        Assert.IsTrue(await new DatabaseExistsHandler(svc).Handle(
            new DatabaseExistsQuery("present"), CT));
    }

    [TestMethod]
    public async Task DatabaseExists_ReturnsFalse_WhenAbsent()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        Assert.IsFalse(await new DatabaseExistsHandler(svc).Handle(
            new DatabaseExistsQuery("absent"), CT));
    }

    // ── List ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetAllDatabaseNames_ReturnsAlphabeticalOrder()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        var createHandler = new CreateDatabaseHandler(svc);
        foreach (var n in new[] { "zebra", "alpha", "middle" })
            await createHandler.Handle(new CreateDatabaseCommand(SeedData.Database(n)), CT);

        var names = await new GetAllDatabaseNamesHandler(svc).Handle(
            new GetAllDatabaseNamesQuery(), CT);

        CollectionAssert.AreEqual(new[] { "alpha", "middle", "zebra" }, names.ToList());
    }

    [TestMethod]
    public async Task GetAllDatabases_ReturnsAllEntries()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        var createHandler = new CreateDatabaseHandler(svc);
        await createHandler.Handle(new CreateDatabaseCommand(SeedData.Database("db1")), CT);
        await createHandler.Handle(new CreateDatabaseCommand(SeedData.Database("db2")), CT);

        var dbs = await new GetAllDatabasesHandler(svc).Handle(new GetAllDatabasesQuery(), CT);

        Assert.HasCount(2, dbs);
    }

    // ── Alter ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AlterDatabase_UpdatesFields()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await new CreateDatabaseHandler(svc).Handle(
            new CreateDatabaseCommand(SeedData.Database("editable")), CT);

        var updated = SeedData.Database("editable");
        updated.Description = "New description";
        updated.OwnerName = "new_owner";
        updated.Parameters = new Dictionary<string, string> { ["key"] = "value" };

        var result = await new AlterDatabaseHandler(svc).Handle(
            new AlterDatabaseCommand("editable", updated), CT);

        Assert.AreEqual("New description", result.Description);
        Assert.AreEqual("new_owner", result.OwnerName);
        Assert.AreEqual("value", result.Parameters["key"]);
    }

    [TestMethod]
    public async Task AlterDatabase_ThrowsNoSuchObject_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new AlterDatabaseHandler(svc).Handle(
                new AlterDatabaseCommand("ghost", SeedData.Database("ghost")), CT));
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task DropDatabase_Succeeds_WhenEmpty()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await new CreateDatabaseHandler(svc).Handle(
            new CreateDatabaseCommand(SeedData.Database("empty_db")), CT);

        await new DropDatabaseHandler(svc).Handle(new DropDatabaseCommand("empty_db", false), CT);

        Assert.IsFalse(await new DatabaseExistsHandler(svc).Handle(
            new DatabaseExistsQuery("empty_db"), CT));
    }

    [TestMethod]
    public async Task DropDatabase_ThrowsNoSuchObject_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new DropDatabaseHandler(svc).Handle(new DropDatabaseCommand("ghost", false), CT));
    }

    [TestMethod]
    public async Task DropDatabase_ThrowsInvalidOperation_WhenHasTables_NoCascade()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await SeedData.SeedTableAsync(ctx, "nonempty");

        await AssertEx.ThrowsAsync<Hmsnet.Core.Exceptions.InvalidOperationException>(() =>
            new DropDatabaseHandler(svc).Handle(new DropDatabaseCommand("nonempty", false), CT));
    }

    [TestMethod]
    public async Task DropDatabase_Succeeds_WithCascade_WhenHasTables()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await SeedData.SeedTableAsync(ctx, "cascadedb");

        await new DropDatabaseHandler(svc).Handle(new DropDatabaseCommand("cascadedb", true), CT);

        Assert.IsFalse(await new DatabaseExistsHandler(svc).Handle(
            new DatabaseExistsQuery("cascadedb"), CT));
    }
}
