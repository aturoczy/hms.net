using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Queries;

public record GetIcebergNamespaceQuery(string Name) : IRequest<HiveDatabase?>;
