using MediatR;

namespace Hmsnet.Core.Features.Databases.Queries;

public record DatabaseExistsQuery(string Name) : IRequest<bool>;
