using Hmsnet.Core.Models;
using MediatR;

namespace Hmsnet.Core.Features.Databases.Queries;

public record GetDatabaseQuery(string Name) : IRequest<HiveDatabase?>;
