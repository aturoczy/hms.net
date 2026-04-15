using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetTableNamesLikeQuery(string DbName, string Pattern) : IRequest<IReadOnlyList<string>>;
