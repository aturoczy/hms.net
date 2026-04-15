using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetTablesBatchQuery(string DbName, List<string> TableNames) : IRequest<IReadOnlyList<HiveTable>>;
