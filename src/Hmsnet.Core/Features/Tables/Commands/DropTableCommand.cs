using MediatR;

namespace Hmsnet.Core.Features.Tables.Commands;

public record DropTableCommand(string DbName, string TableName, bool DeleteData) : IRequest;
