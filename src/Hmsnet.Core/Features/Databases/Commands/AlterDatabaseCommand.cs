using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record AlterDatabaseCommand(string Name, HiveDatabase Updated) : IRequest<HiveDatabase>;
