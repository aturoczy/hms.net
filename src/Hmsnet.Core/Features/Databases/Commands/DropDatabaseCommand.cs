using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record DropDatabaseCommand(string Name, bool Cascade) : IRequest;
