using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Iceberg.Tests.Helpers;

public static class AssertEx
{
    public static async Task ThrowsAsync<TException>(Func<Task> action)
        where TException : Exception
    {
        try
        {
            await action();
            Assert.Fail($"Expected {typeof(TException).Name} but no exception was thrown.");
        }
        catch (TException)
        {
            // expected
        }
        catch (Exception ex)
        {
            Assert.Fail($"Expected {typeof(TException).Name} but got {ex.GetType().Name}: {ex.Message}");
        }
    }
}
