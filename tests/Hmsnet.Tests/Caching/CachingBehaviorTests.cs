using Hmsnet.Core.Caching;
using MediatR;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Caching;

[TestClass]
public class CachingBehaviorTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    private sealed record TestQuery(string Name) : IRequest<int>, ICachedQuery
    {
        public string CacheKey => $"test:{Name}";
        public IReadOnlyCollection<string> Tags => ["test"];
        public TimeSpan Ttl => TimeSpan.FromMinutes(1);
    }

    private sealed record Uncached(string Name) : IRequest<int>;

    [TestMethod]
    public async Task CacheMiss_InvokesHandler_AndStoresResult()
    {
        var cache = new InMemoryCacheService();
        var behavior = new CachingBehavior<TestQuery, int>(cache, NullLogger<CachingBehavior<TestQuery, int>>.Instance);
        var calls = 0;

        RequestHandlerDelegate<int> next = _ => { calls++; return Task.FromResult(123); };

        var result = await behavior.Handle(new TestQuery("a"), next, CT);

        Assert.AreEqual(123, result);
        Assert.AreEqual(1, calls);
        Assert.IsTrue(cache.Contains("test:a"));
    }

    [TestMethod]
    public async Task CacheHit_SkipsHandler()
    {
        var cache = new InMemoryCacheService();
        await cache.SetAsync("test:a", 42, TimeSpan.FromMinutes(1), ["test"], CT);

        var behavior = new CachingBehavior<TestQuery, int>(cache, NullLogger<CachingBehavior<TestQuery, int>>.Instance);
        var calls = 0;
        RequestHandlerDelegate<int> next = _ => { calls++; return Task.FromResult(-1); };

        var result = await behavior.Handle(new TestQuery("a"), next, CT);

        Assert.AreEqual(42, result);
        Assert.AreEqual(0, calls);
    }

    [TestMethod]
    public async Task NonCachedRequest_BypassesCacheEntirely()
    {
        var cache = new InMemoryCacheService();
        var behavior = new CachingBehavior<Uncached, int>(cache, NullLogger<CachingBehavior<Uncached, int>>.Instance);
        RequestHandlerDelegate<int> next = _ => Task.FromResult(7);

        var result = await behavior.Handle(new Uncached("x"), next, CT);

        Assert.AreEqual(7, result);
        Assert.AreEqual(0, cache.SetCount);
        Assert.AreEqual(0, cache.Count);
    }
}
