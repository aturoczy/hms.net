using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record AlterTableCommand(string DbName, string TableName, HiveTable Updated) : IRequest<HiveTable>;
