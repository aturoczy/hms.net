using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Caching;

/// <summary>
/// Tests the in-memory test double itself — if this lies the behavior tests
/// that depend on it are worthless.
/// </summary>
[TestClass]
public class InMemoryCacheServiceTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    [TestMethod]
    public async Task SetThenGet_ReturnsValue()
    {
        var cache = new InMemoryCacheService();
        await cache.SetAsync("k", "v", TimeSpan.FromMinutes(5), ct: CT);

        var (found, value) = await cache.TryGetAsync<string>("k", CT);

        Assert.IsTrue(found);
        Assert.AreEqual("v", value);
    }

    [TestMethod]
    public async Task TryGet_Miss_ReportsNotFound()
    {
        var cache = new InMemoryCacheService();
        var (found, _) = await cache.TryGetAsync<string>("missing", CT);
        Assert.IsFalse(found);
    }

    [TestMethod]
    public async Task InvalidateTag_RemovesAllTaggedKeys()
    {
        var cache = new InMemoryCacheService();
        await cache.SetAsync("k1", 1, TimeSpan.FromMinutes(5), ["tag-a"], CT);
        await cache.SetAsync("k2", 2, TimeSpan.FromMinutes(5), ["tag-a", "tag-b"], CT);
        await cache.SetAsync("k3", 3, TimeSpan.FromMinutes(5), ["tag-b"], CT);

        await cache.InvalidateTagsAsync(["tag-a"], CT);

        Assert.IsFalse(cache.Contains("k1"));
        Assert.IsFalse(cache.Contains("k2"));   // shared tag evicts too
        Assert.IsTrue(cache.Contains("k3"));    // different tag untouched
    }

    [TestMethod]
    public async Task FalseBoolean_IsDistinguishableFromMiss()
    {
        // Regression guard: value types must not collapse "stored false" into "miss".
        var cache = new InMemoryCacheService();
        await cache.SetAsync("flag", false, TimeSpan.FromMinutes(1), ct: CT);

        var (found, value) = await cache.TryGetAsync<bool>("flag", CT);

        Assert.IsTrue(found);
        Assert.IsFalse(value);
    }
}
