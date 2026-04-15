using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Iceberg.Commands;
using Hmsnet.Core.Features.Iceberg.Queries;
using Hmsnet.Core.Models;
using Hmsnet.Iceberg.Tests.Helpers;
using Hmsnet.Infrastructure.Features.Iceberg.Tables;
using Hmsnet.Infrastructure.Services;

namespace Hmsnet.Iceberg.Tests.Features.Iceberg.Tables;

[TestClass]
public class TableHandlerTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private static IcebergCatalogService CreateSvc(Infrastructure.Data.MetastoreDbContext ctx) =>
        new IcebergCatalogService(new DatabaseService(ctx), new TableService(ctx), ctx);

    // ── Create ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task CreateTable_Succeeds_AndStoresMetadata()
    {
        await using var ctx = DbContextFactory.Create();
        var (database, _, _) = await IcebergSeedData.SeedIcebergTableAsync(ctx, "cdb", "existing");
        var svc = CreateSvc(ctx);

        var hiveTable = new HiveTable
        {
            Name = "newtable",
            TableType = TableType.ExternalTable,
            Parameters = new Dictionary<string, string> { ["table_type"] = "ICEBERG" },
            StorageDescriptor = new StorageDescriptor
            {
                Location = "/cdb/newtable",
                SerDeInfo = new SerDeInfo { SerializationLib = "org.apache.iceberg.mr.hive.HiveIcebergSerDe" }
            },
            Columns = [new HiveColumn { Name = "id", TypeName = "long", OrdinalPosition = 0 }],
            Database = new HiveDatabase { Name = "cdb" }
        };
        var metaJson = IcebergSeedData.DefaultMetadataJson;

        var meta = await new CreateIcebergTableHandler(svc).Handle(
            new CreateIcebergTableCommand("cdb", hiveTable, "/cdb/newtable/metadata/v1.metadata.json", metaJson), CT);

        Assert.IsNotNull(meta);
        Assert.IsTrue(meta.Id > 0);
        Assert.AreEqual("/cdb/newtable/metadata/v1.metadata.json", meta.MetadataLocation);
        Assert.IsFalse(string.IsNullOrEmpty(meta.MetadataJson));
    }

    [TestMethod]
    public async Task CreateTable_ThrowsAlreadyExists_ForDuplicateTable()
    {
        await using var ctx = DbContextFactory.Create();
        await IcebergSeedData.SeedIcebergTableAsync(ctx, "dupdb", "testtable");
        var svc = CreateSvc(ctx);

        var hiveTable = new HiveTable
        {
            Name = "testtable",
            TableType = TableType.ExternalTable,
            Parameters = new Dictionary<string, string> { ["table_type"] = "ICEBERG" },
            StorageDescriptor = new StorageDescriptor
            {
                Location = "/dupdb/testtable",
                SerDeInfo = new SerDeInfo { SerializationLib = "lib" }
            },
            Columns = [],
            Database = new HiveDatabase { Name = "dupdb" }
        };

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            new CreateIcebergTableHandler(svc).Handle(
                new CreateIcebergTableCommand("dupdb", hiveTable, "/loc/v1.json", "{}"), CT));
    }

    // ── Load ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task LoadTable_ReturnsStoredMetadata()
    {
        await using var ctx = DbContextFactory.Create();
        await IcebergSeedData.SeedIcebergTableAsync(ctx, "loaddb", "mytable");
        var svc = CreateSvc(ctx);

        var meta = await new LoadIcebergTableHandler(svc).Handle(
            new LoadIcebergTableQuery("loaddb", "mytable"), CT);

        Assert.IsNotNull(meta);
        Assert.AreEqual("/loaddb/mytable/metadata/v1.metadata.json", meta.MetadataLocation);
        Assert.IsFalse(string.IsNullOrEmpty(meta.MetadataJson));
    }

    [TestMethod]
    public async Task LoadTable_ReturnsNull_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = CreateSvc(ctx);

        var meta = await new LoadIcebergTableHandler(svc).Handle(
            new LoadIcebergTableQuery("anydb", "notexist"), CT);

        Assert.IsNull(meta);
    }

    // ── Commit ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task CommitTable_UpdatesMetadataLocationAndJson()
    {
        await using var ctx = DbContextFactory.Create();
        await IcebergSeedData.SeedIcebergTableAsync(ctx, "commitdb", "atable");
        var svc = CreateSvc(ctx);

        var newLocation = "/commitdb/atable/metadata/v2.metadata.json";
        var newJson = IcebergSeedData.DefaultMetadataJson.Replace("test-uuid-1234", "new-uuid-5678");

        var committed = await new CommitIcebergTableHandler(svc).Handle(
            new CommitIcebergTableCommand("commitdb", "atable", newLocation, newJson), CT);

        Assert.AreEqual(newLocation, committed.MetadataLocation);
        Assert.IsTrue(committed.MetadataJson.Contains("new-uuid-5678"));
    }

    [TestMethod]
    public async Task CommitTable_ThrowsNoSuchObject_WhenTableNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = CreateSvc(ctx);

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new CommitIcebergTableHandler(svc).Handle(
                new CommitIcebergTableCommand("anydb", "ghost", "/loc/v2.json", "{}"), CT));
    }

    // ── List ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task ListTables_ReturnsAllTablesInNamespace()
    {
        await using var ctx = DbContextFactory.Create();
        await IcebergSeedData.SeedIcebergTableAsync(ctx, "listdb", "t1");
        await IcebergSeedData.SeedIcebergTableAsync(ctx, "listdb", "t2");
        var svc = CreateSvc(ctx);

        var tables = await new ListIcebergTablesHandler(svc).Handle(
            new ListIcebergTablesQuery("listdb"), CT);

        Assert.AreEqual(2, tables.Count);
        CollectionAssert.IsSubsetOf(
            new[] { "t1", "t2" },
            tables.Select(t => t.Name).ToList());
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task DropTable_Succeeds_AndCleansUpIcebergMetadata()
    {
        await using var ctx = DbContextFactory.Create();
        await IcebergSeedData.SeedIcebergTableAsync(ctx, "dropdb", "droptable");
        var svc = CreateSvc(ctx);

        await new DropIcebergTableHandler(svc).Handle(
            new DropIcebergTableCommand("dropdb", "droptable", false), CT);

        var meta = await new LoadIcebergTableHandler(svc).Handle(
            new LoadIcebergTableQuery("dropdb", "droptable"), CT);

        Assert.IsNull(meta);
    }

    [TestMethod]
    public async Task DropTable_ThrowsNoSuchObject_WhenNotFound()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = CreateSvc(ctx);

        await AssertEx.ThrowsAsync<NoSuchObjectException>(() =>
            new DropIcebergTableHandler(svc).Handle(
                new DropIcebergTableCommand("anydb", "ghost", false), CT));
    }

    // ── Register ──────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task RegisterTable_Succeeds_WithExistingNamespace()
    {
        await using var ctx = DbContextFactory.Create();
        await IcebergSeedData.SeedIcebergTableAsync(ctx, "regdb", "seed");
        var svc = CreateSvc(ctx);

        var meta = await new RegisterIcebergTableHandler(svc).Handle(
            new RegisterIcebergTableCommand("regdb", "registered", "/regdb/registered/metadata/v1.json", "{}"), CT);

        Assert.IsNotNull(meta);
        Assert.AreEqual("/regdb/registered/metadata/v1.json", meta.MetadataLocation);
    }

    // ── Rename ────────────────────────────────────────────────────────────────

    [TestMethod]
    public async Task RenameTable_Succeeds_WithinSameNamespace()
    {
        await using var ctx = DbContextFactory.Create();
        await IcebergSeedData.SeedIcebergTableAsync(ctx, "renamedb", "oldname");
        var svc = CreateSvc(ctx);

        await new RenameIcebergTableHandler(svc).Handle(
            new RenameIcebergTableCommand("renamedb", "oldname", "renamedb", "newname"), CT);

        var newMeta = await new LoadIcebergTableHandler(svc).Handle(
            new LoadIcebergTableQuery("renamedb", "newname"), CT);
        var oldMeta = await new LoadIcebergTableHandler(svc).Handle(
            new LoadIcebergTableQuery("renamedb", "oldname"), CT);

        Assert.IsNotNull(newMeta);
        Assert.IsNull(oldMeta);
    }
}
