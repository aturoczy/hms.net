using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetFieldsQuery(string DbName, string TableName) : IRequest<IReadOnlyList<HiveColumn>>;
