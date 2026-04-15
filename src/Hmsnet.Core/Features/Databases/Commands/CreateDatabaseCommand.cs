using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Commands;

public record CreateDatabaseCommand(HiveDatabase Database) : IRequest<HiveDatabase>;
