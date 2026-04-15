using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Tables.Commands;
using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Features.Tables;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Features.Tables;

[TestClass]
public class TableHandlerTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    // ── Create ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task CreateTable_Succeeds_AndAssignsId()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var database = await SeedData.SeedDatabaseAsync(ctx, "db1");

        var table = SeedData.Table(database.Id, "db1", "orders");
        table.Database = database;
        var created = await new CreateTableHandler(svc).Handle(new CreateTableCommand(table), CT);

        Assert.IsTrue(created.Id > 0);
        Assert.AreEqual("orders", created.Name);
    }

    [TestMethod]
    public async Task CreateTable_NormalizesNameToLowercase()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var database = await SeedData.SeedDatabaseAsync(ctx, "db1");

        var table = SeedData.Table(database.Id, "db1", "UPPER_TABLE");
        table.Database = database;
        var created = await new CreateTableHandler(svc).Handle(new CreateTableCommand(table), CT);

        Assert.AreEqual("upper_table", created.Name);
    }

    [TestMethod]
    public async Task CreateTable_ThrowsNoSuchObject_WhenDbNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);

        var table = SeedData.Table(999, "ghostdb", "t");
        table.Database = new HiveDatabase { Name = "ghostdb" };

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new CreateTableHandler(svc).Handle(new CreateTableCommand(table), CT));
    }

    [TestMethod]
    public async Task CreateTable_ThrowsAlreadyExists_ForDuplicate()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var (db, _) = await SeedData.SeedTableAsync(ctx, "db1", "dup_table");

        var table2 = SeedData.Table(db.Id, "db1", "dup_table");
        table2.Database = db;

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            new CreateTableHandler(svc).Handle(new CreateTableCommand(table2), CT));
    }

    // ── Get / Exists ──────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetTable_ReturnsCorrectTable()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "mydb", "mytable");

        var table = await new GetTableHandler(svc).Handle(new GetTableQuery("mydb", "mytable"), CT);

        Assert.IsNotNull(table);
        Assert.AreEqual("mytable", table.Name);
    }

    [TestMethod]
    public async Task GetTable_ReturnsNull_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "mydb");

        var table = await new GetTableHandler(svc).Handle(new GetTableQuery("mydb", "ghost"), CT);

        Assert.IsNull(table);
    }

    [TestMethod]
    public async Task GetTable_IsCaseInsensitive()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "mydb", "mytable");

        var table = await new GetTableHandler(svc).Handle(new GetTableQuery("MYDB", "MYTABLE"), CT);

        Assert.IsNotNull(table);
    }

    [TestMethod]
    public async Task TableExists_ReturnsTrue_WhenPresent()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "mydb", "t1");

        Assert.IsTrue(await new TableExistsHandler(svc).Handle(new TableExistsQuery("mydb", "t1"), CT));
    }

    [TestMethod]
    public async Task TableExists_ReturnsFalse_WhenAbsent()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "mydb");

        Assert.IsFalse(await new TableExistsHandler(svc).Handle(new TableExistsQuery("mydb", "ghost"), CT));
    }

    // ── List ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetAllTableNames_ReturnsAlphabeticalOrder()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var db = await SeedData.SeedDatabaseAsync(ctx, "db1");
        foreach (var n in new[] { "zebra", "alpha", "middle" })
        {
            var t = SeedData.Table(db.Id, "db1", n);
            t.Database = db;
            ctx.Tables.Add(t);
        }
        await ctx.SaveChangesAsync();

        var names = await new GetAllTableNamesHandler(svc).Handle(new GetAllTableNamesQuery("db1"), CT);

        CollectionAssert.AreEqual(new[] { "alpha", "middle", "zebra" }, names.ToList());
    }

    [TestMethod]
    public async Task GetTableNamesLike_ReturnsMatchingTables()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var db = await SeedData.SeedDatabaseAsync(ctx, "db1");
        foreach (var n in new[] { "sales_2023", "sales_2024", "orders" })
        {
            var t = SeedData.Table(db.Id, "db1", n);
            t.Database = db;
            ctx.Tables.Add(t);
        }
        await ctx.SaveChangesAsync();

        var names = await new GetTableNamesLikeHandler(svc).Handle(
            new GetTableNamesLikeQuery("db1", "sales*"), CT);

        Assert.AreEqual(2, names.Count);
        Assert.IsTrue(names.All(n => n.StartsWith("sales")));
    }

    [TestMethod]
    public async Task GetTablesBatch_ReturnsOnlyRequested()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var db = await SeedData.SeedDatabaseAsync(ctx, "db1");
        foreach (var n in new[] { "t1", "t2", "t3" })
        {
            var t = SeedData.Table(db.Id, "db1", n);
            t.Database = db;
            ctx.Tables.Add(t);
        }
        await ctx.SaveChangesAsync();

        var tables = await new GetTablesBatchHandler(svc).Handle(
            new GetTablesBatchQuery("db1", ["t1", "t3"]), CT);

        Assert.AreEqual(2, tables.Count);
        CollectionAssert.Contains(tables.Select(t => t.Name).ToList(), "t1");
        CollectionAssert.Contains(tables.Select(t => t.Name).ToList(), "t3");
    }

    // ── Schema ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetFields_ReturnsOnlyDataColumns()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1",
            extraPartKeys: SeedData.DefaultPartitionKeys());

        var fields = await new GetFieldsHandler(svc).Handle(new GetFieldsQuery("db1", "t1"), CT);

        Assert.IsTrue(fields.All(f => !f.IsPartitionKey));
        Assert.AreEqual(2, fields.Count); // id, name
    }

    [TestMethod]
    public async Task GetSchema_ReturnsAllColumns_DataFirst()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "t1",
            extraPartKeys: SeedData.DefaultPartitionKeys());

        var schema = await new GetSchemaHandler(svc).Handle(new GetSchemaQuery("db1", "t1"), CT);

        Assert.AreEqual(3, schema.Count); // id, name, dt
        Assert.IsFalse(schema.First().IsPartitionKey);
        Assert.IsTrue(schema.Last().IsPartitionKey);
    }

    [TestMethod]
    public async Task GetFields_ThrowsNoSuchObject_WhenTableMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "db1");

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new GetFieldsHandler(svc).Handle(new GetFieldsQuery("db1", "ghost"), CT));
    }

    // ── Alter ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AlterTable_UpdatesMetadata()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var (db, _) = await SeedData.SeedTableAsync(ctx, "db1", "t1");

        var updated = SeedData.Table(db.Id, "db1", "t1");
        updated.Owner = "new_owner";
        updated.Parameters = new Dictionary<string, string> { ["comment"] = "updated" };
        updated.StorageDescriptor = SeedData.DefaultSd("hdfs:///new/location");
        updated.Columns = SeedData.DefaultColumns();
        updated.Database = db;

        var result = await new AlterTableHandler(svc).Handle(
            new AlterTableCommand("db1", "t1", updated), CT);

        Assert.AreEqual("new_owner", result.Owner);
        Assert.AreEqual("updated", result.Parameters["comment"]);
        Assert.AreEqual("hdfs:///new/location", result.StorageDescriptor.Location);
    }

    [TestMethod]
    public async Task AlterTable_ThrowsNoSuchObject_WhenMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var db = await SeedData.SeedDatabaseAsync(ctx, "db1");

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new AlterTableHandler(svc).Handle(
                new AlterTableCommand("db1", "ghost", SeedData.Table(db.Id, "db1", "ghost")), CT));
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task DropTable_Succeeds()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "removeme");

        await new DropTableHandler(svc).Handle(new DropTableCommand("db1", "removeme", false), CT);

        Assert.IsFalse(await new TableExistsHandler(svc).Handle(new TableExistsQuery("db1", "removeme"), CT));
    }

    [TestMethod]
    public async Task DropTable_ThrowsNoSuchObject_WhenMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "db1");

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new DropTableHandler(svc).Handle(new DropTableCommand("db1", "ghost", false), CT));
    }
}
