using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests;

[TestClass]
public class TableServiceTests
{
    // ── Create ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task CreateTable_Succeeds_AndAssignsId()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var database = await SeedData.SeedDatabaseAsync(ctx, "db1");

        var table = SeedData.Table(database.Id, "db1", "orders");
        table.Database = database;
        var created = await svc.CreateTableAsync(table);

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
        var created = await svc.CreateTableAsync(table);

        Assert.AreEqual("upper_table", created.Name);
    }

    [TestMethod]
    public async Task CreateTable_SetsDefaultLocation_WhenEmpty()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var database = await SeedData.SeedDatabaseAsync(ctx, "db1");

        var table = SeedData.Table(database.Id, "db1", "mytable");
        table.Database = database;
        table.StorageDescriptor.Location = string.Empty;
        var created = await svc.CreateTableAsync(table);

        StringAssert.Contains(created.StorageDescriptor.Location, "mytable");
    }

    [TestMethod]
    public async Task CreateTable_ThrowsNoSuchObject_WhenDbNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);

        var table = SeedData.Table(999, "ghostdb", "t");
        table.Database = new HiveDatabase { Name = "ghostdb" };

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            svc.CreateTableAsync(table));
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
            svc.CreateTableAsync(table2));
    }

    [TestMethod]
    public async Task CreateTable_StoresPartitionKeys_Correctly()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var db = await SeedData.SeedDatabaseAsync(ctx, "db1");

        var partKeys = SeedData.DefaultPartitionKeys();
        var table = SeedData.Table(db.Id, "db1", "partitioned", partitionKeys: partKeys);
        table.Database = db;
        var created = await svc.CreateTableAsync(table);

        var partitioned = await svc.GetTableAsync("db1", "partitioned");
        Assert.IsNotNull(partitioned);
        Assert.IsTrue(partitioned.PartitionKeys.Any());
        Assert.AreEqual("dt", partitioned.PartitionKeys.First().Name);
    }

    // ── Get / Exists ──────────────────────────────────────────────────────────

    [TestMethod]
    public async Task GetTable_ReturnsCorrectTable()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "mydb", "mytable");

        var table = await svc.GetTableAsync("mydb", "mytable");

        Assert.IsNotNull(table);
        Assert.AreEqual("mytable", table.Name);
    }

    [TestMethod]
    public async Task GetTable_ReturnsNull_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "mydb");

        var table = await svc.GetTableAsync("mydb", "ghost");

        Assert.IsNull(table);
    }

    [TestMethod]
    public async Task GetTable_IsCaseInsensitive()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "mydb", "mytable");

        var table = await svc.GetTableAsync("MYDB", "MYTABLE");

        Assert.IsNotNull(table);
    }

    [TestMethod]
    public async Task TableExists_ReturnsTrue_WhenPresent()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "mydb", "t1");

        Assert.IsTrue(await svc.TableExistsAsync("mydb", "t1"));
    }

    [TestMethod]
    public async Task TableExists_ReturnsFalse_WhenAbsent()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "mydb");

        Assert.IsFalse(await svc.TableExistsAsync("mydb", "ghost"));
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

        var names = await svc.GetAllTableNamesAsync("db1");

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

        var names = await svc.GetTableNamesLikeAsync("db1", "sales*");

        Assert.AreEqual(2, names.Count);
        Assert.IsTrue(names.All(n => n.StartsWith("sales")));
    }

    [TestMethod]
    public async Task GetTablesInBatch_ReturnsOnlyRequested()
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

        var tables = await svc.GetTablesAsync("db1", ["t1", "t3"]);

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

        var fields = await svc.GetFieldsAsync("db1", "t1");

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

        var schema = await svc.GetSchemaAsync("db1", "t1");

        Assert.AreEqual(3, schema.Count); // id, name, dt
        // Data columns must come before partition keys
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
            svc.GetFieldsAsync("db1", "ghost"));
    }

    // ── Alter ─────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task AlterTable_UpdatesMetadata()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        var (db, original) = await SeedData.SeedTableAsync(ctx, "db1", "t1");

        var updated = SeedData.Table(db.Id, "db1", "t1");
        updated.Owner = "new_owner";
        updated.Parameters = new Dictionary<string, string> { ["comment"] = "updated" };
        updated.StorageDescriptor = SeedData.DefaultSd("hdfs:///new/location");
        updated.Columns = SeedData.DefaultColumns();
        updated.Database = db;

        var result = await svc.AlterTableAsync("db1", "t1", updated);

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
            svc.AlterTableAsync("db1", "ghost", SeedData.Table(db.Id, "db1", "ghost")));
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task DropTable_Succeeds()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedTableAsync(ctx, "db1", "removeme");

        await svc.DropTableAsync("db1", "removeme", deleteData: false);

        Assert.IsFalse(await svc.TableExistsAsync("db1", "removeme"));
    }

    [TestMethod]
    public async Task DropTable_ThrowsNoSuchObject_WhenMissing()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new TableService(ctx);
        await SeedData.SeedDatabaseAsync(ctx, "db1");

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            svc.DropTableAsync("db1", "ghost", deleteData: false));
    }
}
