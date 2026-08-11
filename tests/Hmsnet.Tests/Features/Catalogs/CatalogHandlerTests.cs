using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Catalogs.Commands;
using Hmsnet.Core.Features.Catalogs.Queries;
using Hmsnet.Core.Models;
using Hmsnet.Infrastructure.Features.Catalogs;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;

namespace Hmsnet.Tests.Features.Catalogs;

[TestClass]
public class CatalogHandlerTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private static Catalog New(string name) => new()
    {
        Name = name,
        Description = $"{name} catalog",
        LocationUri = $"hdfs:///warehouses/{name}",
        Properties = new Dictionary<string, string> { ["owner"] = "data-team" }
    };

    [TestMethod]
    public async Task CreateCatalog_Succeeds_AndLowercasesName()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new CatalogService(ctx);

        var cat = await new CreateCatalogHandler(svc).Handle(
            new CreateCatalogCommand(New("SPARK")), CT);

        Assert.AreEqual("spark", cat.Name);
        Assert.AreEqual("data-team", cat.Properties["owner"]);
    }

    [TestMethod]
    public async Task CreateCatalog_DuplicateName_ThrowsAlreadyExists()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new CatalogService(ctx);
        var handler = new CreateCatalogHandler(svc);
        await handler.Handle(new CreateCatalogCommand(New("dup")), CT);

        await AssertEx.ThrowsAsync<AlreadyExistsException>(() =>
            handler.Handle(new CreateCatalogCommand(New("dup")), CT));
    }

    [TestMethod]
    public async Task AlterCatalog_UpdatesProperties()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new CatalogService(ctx);
        await new CreateCatalogHandler(svc).Handle(new CreateCatalogCommand(New("editable")), CT);

        var updated = New("editable");
        updated.Description = "new";
        updated.Properties = new Dictionary<string, string> { ["owner"] = "platform" };

        var result = await new AlterCatalogHandler(svc).Handle(
            new AlterCatalogCommand("editable", updated), CT);

        Assert.AreEqual("new", result.Description);
        Assert.AreEqual("platform", result.Properties["owner"]);
    }

    [TestMethod]
    public async Task DropCatalog_DefaultHive_Rejected()
    {
        // The seed migration inserts the default catalog "hive"; that row is
        // absent from the in-memory context, but the service still refuses by name.
        await using var ctx = DbContextFactory.Create();
        var svc = new CatalogService(ctx);

        await AssertEx.ThrowsAsync<Hmsnet.Core.Exceptions.InvalidOperationException>(() =>
            new DropCatalogHandler(svc).Handle(new DropCatalogCommand(Catalog.DefaultName), CT));
    }

    [TestMethod]
    public async Task GetAllCatalogNames_ReturnsSortedNames()
    {
        await using var ctx = DbContextFactory.Create();
        var svc = new CatalogService(ctx);
        var create = new CreateCatalogHandler(svc);
        foreach (var n in new[] { "zebra", "alpha", "middle" })
            await create.Handle(new CreateCatalogCommand(New(n)), CT);

        var names = await new GetAllCatalogNamesHandler(svc).Handle(
            new GetAllCatalogNamesQuery(), CT);

        CollectionAssert.AreEqual(new[] { "alpha", "middle", "zebra" }, names.ToList());
    }
}
