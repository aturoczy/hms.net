using Hmsnet.Core.Caching;
using Hmsnet.Core.Features.Databases.Commands;
using Hmsnet.Core.Features.Notifications.Queries;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Notifications;
using Hmsnet.Infrastructure.Notifications;
using Hmsnet.Infrastructure.Services;
using Hmsnet.Tests.Caching;
using Hmsnet.Tests.Helpers;
using MediatR;
using Microsoft.Extensions.DependencyInjection;

namespace Hmsnet.Tests.Features.Notifications;

[TestClass]
public class NotificationEventTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private static (IMediator mediator, INotificationEventSink sink) BuildPipeline()
    {
        var ctx = DbContextFactory.Create();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ICacheService>(new InMemoryCacheService());
        services.AddSingleton(ctx);
        services.AddSingleton<IDatabaseService, DatabaseService>();
        services.AddSingleton<INotificationEventSink, DbNotificationEventSink>();
        services.AddMediatR(cfg =>
        {
            cfg.RegisterServicesFromAssembly(
                typeof(Hmsnet.Infrastructure.Features.Databases.CreateDatabaseHandler).Assembly);
            cfg.AddOpenBehavior(typeof(NotificationEmissionBehavior<,>));
        });

        var sp = services.BuildServiceProvider();
        return (sp.GetRequiredService<IMediator>(), sp.GetRequiredService<INotificationEventSink>());
    }

    [TestMethod]
    public async Task Create_Alter_Drop_ProducesEventStream()
    {
        var (mediator, sink) = BuildPipeline();

        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("obs")), CT);
        await mediator.Send(new AlterDatabaseCommand("obs", SeedData.Database("obs")), CT);
        await mediator.Send(new DropDatabaseCommand("obs", Cascade: false), CT);

        var events = await sink.ReadAsync(afterId: 0, maxEvents: 100, CT);

        Assert.HasCount(3, events);
        CollectionAssert.AreEqual(
            new[] { "CREATE_DATABASE", "ALTER_DATABASE", "DROP_DATABASE" },
            events.Select(e => e.EventType).ToList());
        Assert.IsTrue(events.All(e => e.DbName == "obs"));
    }

    [TestMethod]
    public async Task GetEvents_WithAfterCursor_ReturnsOnlyNewer()
    {
        var (mediator, sink) = BuildPipeline();

        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("first")), CT);
        var checkpoint = await sink.GetCurrentEventIdAsync(CT);
        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("second")), CT);

        var newer = await sink.ReadAsync(afterId: checkpoint, maxEvents: 100, CT);

        Assert.HasCount(1, newer);
        Assert.AreEqual("second", newer[0].DbName);
    }

    [TestMethod]
    public async Task ReadEvents_HandlerThrows_LeavesEventStreamUnchanged()
    {
        var (mediator, sink) = BuildPipeline();

        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("keep")), CT);

        // Duplicate create → throws inside the handler; behavior must not emit.
        try
        {
            await mediator.Send(new CreateDatabaseCommand(SeedData.Database("keep")), CT);
        }
        catch { /* expected */ }

        var events = await sink.ReadAsync(afterId: 0, maxEvents: 100, CT);
        Assert.HasCount(1, events);
    }

    [TestMethod]
    public async Task GetNotificationEventsQuery_ThroughMediator_ReturnsBatch()
    {
        var (mediator, _) = BuildPipeline();
        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("q1")), CT);
        await mediator.Send(new CreateDatabaseCommand(SeedData.Database("q2")), CT);

        var batch = await mediator.Send(new GetNotificationEventsQuery(AfterId: 0, MaxEvents: 100), CT);

        Assert.HasCount(2, batch);
    }
}
