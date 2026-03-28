using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Hmsnet.Tests.Helpers;

/// <summary>
/// Async exception assertion helper, compatible with MSTest 4.x where
/// <c>Assert.ThrowsExceptionAsync</c> was renamed to <c>Assert.ThrowsExactlyAsync</c>.
/// </summary>
public static class AssertEx
{
    /// <summary>
    /// Asserts that the given async delegate throws exactly <typeparamref name="TException"/>
    /// (not a derived type). Use <see cref="ThrowsAsync{TException}"/> when derived types are acceptable.
    /// </summary>
    public static async Task ThrowsExactlyAsync<TException>(Func<Task> action)
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

    /// <summary>
    /// Asserts that the given async delegate throws <typeparamref name="TException"/>
    /// or any subclass of it.
    /// </summary>
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
