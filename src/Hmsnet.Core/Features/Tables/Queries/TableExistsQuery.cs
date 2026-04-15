using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record TableExistsQuery(string DbName, string TableName) : IRequest<bool>;
