using MediatR;

namespace Hmsnet.Core.Features.Databases.Queries;

public record GetAllDatabaseNamesQuery() : IRequest<IReadOnlyList<string>>;
