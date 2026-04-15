using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record CreateTableCommand(HiveTable Table) : IRequest<HiveTable>;
