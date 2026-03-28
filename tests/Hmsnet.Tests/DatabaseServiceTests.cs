using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests;

[TestClass]
public class DatabaseServiceTests
{
    // ── Create ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task CreateDatabase_Succeeds_AndAssignsId()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        var db = await svc.CreateDatabaseAsync(SeedData.Database("sales"));

        Assert.IsGreaterThan(0, db.Id);
        Assert.AreEqual("sales", db.Name);
        Assert.IsGreaterThan(0, db.CreateTime);
    }

    [TestMethod]
    public async Task CreateDatabase_NormalizesNameToLowercase()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        var db = await svc.CreateDatabaseAsync(SeedData.Database("UPPERCASE_DB"));

        Assert.AreEqual("uppercase_db", db.Name);
    }

    [TestMethod]
    public async Task CreateDatabase_SetsDefaultLocationUri_WhenEmpty()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        var entity = SeedData.Database("mydb");
        entity.LocationUri = string.Empty;

        var db = await svc.CreateDatabaseAsync(entity);

        StringAssert.Contains(db.LocationUri, "mydb");
    }

    [TestMethod]
    public async Task CreateDatabase_ThrowsAlreadyExists_ForDuplicateName()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await svc.CreateDatabaseAsync(SeedData.Database("dup"));

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            svc.CreateDatabaseAsync(SeedData.Database("dup")));
    }

    [TestMethod]
    public async Task CreateDatabase_ThrowsAlreadyExists_CaseInsensitive()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await svc.CreateDatabaseAsync(SeedData.Database("mydb"));

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            svc.CreateDatabaseAsync(SeedData.Database("MYDB")));
    }

    // ── Get / Exists ──────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetDatabase_ReturnsCorrectDatabase()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await svc.CreateDatabaseAsync(SeedData.Database("orders"));

        var db = await svc.GetDatabaseAsync("orders");

        Assert.IsNotNull(db);
        Assert.AreEqual("orders", db.Name);
    }

    [TestMethod]
    public async Task GetDatabase_ReturnsNull_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        var db = await svc.GetDatabaseAsync("nonexistent");

        Assert.IsNull(db);
    }

    [TestMethod]
    public async Task GetDatabase_IsCaseInsensitive()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await svc.CreateDatabaseAsync(SeedData.Database("mydb"));

        var db = await svc.GetDatabaseAsync("MYDB");

        Assert.IsNotNull(db);
    }

    [TestMethod]
    public async Task DatabaseExists_ReturnsTrue_WhenPresent()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await svc.CreateDatabaseAsync(SeedData.Database("present"));

        Assert.IsTrue(await svc.DatabaseExistsAsync("present"));
    }

    [TestMethod]
    public async Task DatabaseExists_ReturnsFalse_WhenAbsent()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        Assert.IsFalse(await svc.DatabaseExistsAsync("absent"));
    }

    // ── List ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetAllDatabaseNames_ReturnsAlphabeticalOrder()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        foreach (var n in new[] { "zebra", "alpha", "middle" })
            await svc.CreateDatabaseAsync(SeedData.Database(n));

        var names = await svc.GetAllDatabaseNamesAsync();

        CollectionAssert.AreEqual(new[] { "alpha", "middle", "zebra" }, names.ToList());
    }

    [TestMethod]
    public async Task GetAllDatabases_ReturnsAllEntries()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await svc.CreateDatabaseAsync(SeedData.Database("db1"));
        await svc.CreateDatabaseAsync(SeedData.Database("db2"));

        var dbs = await svc.GetAllDatabasesAsync();

        Assert.HasCount(2, dbs);
    }

    // ── Alter ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AlterDatabase_UpdatesFields()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await svc.CreateDatabaseAsync(SeedData.Database("editable"));

        var updated = SeedData.Database("editable");
        updated.Description = "New description";
        updated.OwnerName = "new_owner";
        updated.Parameters = new Dictionary<string, string> { ["key"] = "value" };

        var result = await svc.AlterDatabaseAsync("editable", updated);

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
            svc.AlterDatabaseAsync("ghost", SeedData.Database("ghost")));
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task DropDatabase_Succeeds_WhenEmpty()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await svc.CreateDatabaseAsync(SeedData.Database("empty_db"));

        await svc.DropDatabaseAsync("empty_db", cascade: false);

        Assert.IsFalse(await svc.DatabaseExistsAsync("empty_db"));
    }

    [TestMethod]
    public async Task DropDatabase_ThrowsNoSuchObject_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            svc.DropDatabaseAsync("ghost", cascade: false));
    }

    [TestMethod]
    public async Task DropDatabase_ThrowsInvalidOperation_WhenHasTables_NoCascade()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        var (_, _) = await SeedData.SeedTableAsync(ctx, "nonempty");

        await AssertEx.ThrowsAsync<Hmsnet.Core.Exceptions.InvalidOperationException>(() =>
            svc.DropDatabaseAsync("nonempty", cascade: false));
    }

    [TestMethod]
    public async Task DropDatabase_Succeeds_WithCascade_WhenHasTables()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new DatabaseService(ctx);
        await SeedData.SeedTableAsync(ctx, "cascadedb");

        await svc.DropDatabaseAsync("cascadedb", cascade: true);

        Assert.IsFalse(await svc.DatabaseExistsAsync("cascadedb"));
    }
}
