using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Queries;

public record ListIcebergNamespacesQuery : IRequest<IReadOnlyList<HiveDatabase>>;
