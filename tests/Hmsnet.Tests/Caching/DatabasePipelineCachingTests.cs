using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Databases.Commands;
using Hmsnet.Core.Features.Databases.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Helpers;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Caching;

/// <summary>
/// End-to-end sanity check: the MediatR pipeline must serve repeat reads from
/// the cache, and a write command must evict the cache so the next read goes
/// back to the database. This exercises the complete wiring
/// (behaviors + handlers + EF-backed service) with the in-memory cache double.
/// </summary>
[TestClass]
public class DatabasePipelineCachingTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private static (IMediator mediator, InMemoryCacheService cache) BuildPipeline()
    {
        var ctx = DbContextFactory.Create();
        var cache = new InMemoryCacheService();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ICacheService>(cache);
        services.AddSingleton(ctx);
        services.AddSingleton<IDatabaseService, DatabaseService>();
        services.AddMediatR(cfg =>
        {
            cfg.RegisterServicesFromAssembly(
                typeof(Hmsnet.Infrastructure.Features.Databases.CreateDatabaseHandler).Assembly);
            cfg.AddOpenBehavior(typeof(CachingBehavior<,>));
            cfg.AddOpenBehavior(typeof(InvalidationBehavior<,>));
        });

        var provider = services.BuildServiceProvider();
        return (provider.GetRequiredService<IMediator>(), cache);
    }

    [TestMethod]
    public async Task SecondRead_ComesFromCache()
    {
        var (mediator, cache) = BuildPipeline();
        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("alpha")), CT);

        var first = await mediator.Send(new GetAllDatabaseNamesQuery(), CT);
        var setsAfterFirst = cache.SetCount;
        var second = await mediator.Send(new GetAllDatabaseNamesQuery(), CT);

        CollectionAssert.AreEqual(first.ToList(), second.ToList());
        Assert.AreEqual(setsAfterFirst, cache.SetCount,
            "Second read should be served from cache and not re-store the entry.");
    }

    [TestMethod]
    public async Task CreateDatabase_EvictsDatabaseListCache()
    {
        var (mediator, cache) = BuildPipeline();

        // Prime the cache with the initial (empty) list.
        await mediator.Send(new GetAllDatabaseNamesQuery(), CT);
        Assert.IsTrue(cache.Count > 0);

        // Write should invalidate the tag → subsequent read reflects the new row.
        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("beta")), CT);

        Assert.IsTrue(cache.InvalidationCount > 0);

        var names = await mediator.Send(new GetAllDatabaseNamesQuery(), CT);
        CollectionAssert.Contains(names.ToList(), "beta");
    }

    [TestMethod]
    public async Task DropDatabase_EvictsSingleDatabaseLookup()
    {
        var (mediator, cache) = BuildPipeline();
        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("gamma")), CT);

        // Prime single-entry lookup.
        var before = await mediator.Send(new GetDatabaseQuery("gamma"), CT);
        Assert.IsNotNull(before);

        await mediator.Send(new DropDatabaseCommand("gamma", false), CT);

        var after = await mediator.Send(new GetDatabaseQuery("gamma"), CT);
        Assert.IsNull(after, "Dropped database must not be served stale from the cache.");
    }
}
