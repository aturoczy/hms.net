using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Queries;

public record GetAllDatabasesQuery() : IRequest<IReadOnlyList<HiveDatabase>>;
