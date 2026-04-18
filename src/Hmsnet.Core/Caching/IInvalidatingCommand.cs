namespace Hmsnet.Core.Caching;

/// <summary>
/// Marker implemented by MediatR commands that mutate state tracked in the
/// cache. After the handler completes successfully the pipeline
/// <c>InvalidationBehavior</c> evicts every key filed under one of the
/// returned tags.
/// </summary>
public interface IInvalidatingCommand
{
    /// <summary>Cache tags the write invalidates.</summary>
    IReadOnlyCollection<string> InvalidatesTags { get; }
}
