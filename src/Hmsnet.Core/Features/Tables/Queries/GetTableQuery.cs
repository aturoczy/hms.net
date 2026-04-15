using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Queries;

public record GetTableQuery(string DbName, string TableName) : IRequest<HiveTable?>;
