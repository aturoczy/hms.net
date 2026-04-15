using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Queries;

public record ListIcebergTablesQuery(string DbName) : IRequest<IReadOnlyList<HiveTable>>;
