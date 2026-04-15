using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetAllTablesQuery(string DbName) : IRequest<IReadOnlyList<HiveTable>>;
