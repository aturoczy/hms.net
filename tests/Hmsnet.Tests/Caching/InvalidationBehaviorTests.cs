using Hmsnet.Core.Caching;
using Hmsnet.Tests.Helpers;
using MediatR;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Caching;

[TestClass]
public class InvalidationBehaviorTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private sealed record InvalidateCmd : IRequest<Unit>, IInvalidatingCommand
    {
        public IReadOnlyCollection<string> InvalidatesTags { get; init; } = ["tag-a"];
    }

    private sealed record PlainCmd : IRequest<Unit>;

    [TestMethod]
    public async Task EvictsTags_AfterSuccessfulHandler()
    {
        var cache = new InMemoryCacheService();
        await cache.SetAsync("k1", 1, TimeSpan.FromMinutes(5), ["tag-a"], CT);
        await cache.SetAsync("k2", 2, TimeSpan.FromMinutes(5), ["tag-b"], CT);

        var behavior = new InvalidationBehavior<InvalidateCmd, Unit>(
            cache, NullLogger<InvalidationBehavior<InvalidateCmd, Unit>>.Instance);

        RequestHandlerDelegate<Unit> next = _ => Task.FromResult(Unit.Value);

        await behavior.Handle(new InvalidateCmd(), next, CT);

        Assert.IsFalse(cache.Contains("k1"));
        Assert.IsTrue(cache.Contains("k2"));   // unrelated tag untouched
        Assert.AreEqual(1, cache.InvalidationCount);
    }

    [TestMethod]
    public async Task HandlerThrows_LeavesCacheIntact()
    {
        var cache = new InMemoryCacheService();
        await cache.SetAsync("k1", 1, TimeSpan.FromMinutes(5), ["tag-a"], CT);

        var behavior = new InvalidationBehavior<InvalidateCmd, Unit>(
            cache, NullLogger<InvalidationBehavior<InvalidateCmd, Unit>>.Instance);

        RequestHandlerDelegate<Unit> next = _ => throw new InvalidOperationException("nope");

        await AssertEx.ThrowsAsync<InvalidOperationException>(() =>
            behavior.Handle(new InvalidateCmd(), next, CT));

        Assert.IsTrue(cache.Contains("k1"));
        Assert.AreEqual(0, cache.InvalidationCount);
    }

    [TestMethod]
    public async Task NonInvalidatingCommand_DoesNotTouchCache()
    {
        var cache = new InMemoryCacheService();
        await cache.SetAsync("k1", 1, TimeSpan.FromMinutes(5), ["tag-a"], CT);

        var behavior = new InvalidationBehavior<PlainCmd, Unit>(
            cache, NullLogger<InvalidationBehavior<PlainCmd, Unit>>.Instance);

        RequestHandlerDelegate<Unit> next = _ => Task.FromResult(Unit.Value);
        await behavior.Handle(new PlainCmd(), next, CT);

        Assert.IsTrue(cache.Contains("k1"));
        Assert.AreEqual(0, cache.InvalidationCount);
    }
}
