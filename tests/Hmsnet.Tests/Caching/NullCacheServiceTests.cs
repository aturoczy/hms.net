using Hmsnet.Core.Caching;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Caching;

[TestClass]
public class NullCacheServiceTests
{
    private static readonly CancellationToken CT = CancellationToken.None;

    [TestMethod]
    public async Task TryGet_AlwaysReportsMiss()
    {
        var cache = new NullCacheService();
        await cache.SetAsync("foo", 42, TimeSpan.FromMinutes(1), ct: CT);

        var (found, value) = await cache.TryGetAsync<int>("foo", CT);

        Assert.IsFalse(found);
        Assert.AreEqual(0, value);
    }

    [TestMethod]
    public async Task Invalidate_IsNoOp()
    {
        var cache = new NullCacheService();
        // Must not throw even though no key was ever stored.
        await cache.InvalidateTagsAsync(["any"], CT);
        await cache.RemoveAsync("whatever", CT);
    }
}
