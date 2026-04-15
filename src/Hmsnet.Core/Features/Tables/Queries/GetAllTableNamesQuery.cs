using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetAllTableNamesQuery(string DbName) : IRequest<IReadOnlyList<string>>;
