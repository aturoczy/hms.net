using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetSchemaQuery(string DbName, string TableName) : IRequest<IReadOnlyList<HiveColumn>>;
