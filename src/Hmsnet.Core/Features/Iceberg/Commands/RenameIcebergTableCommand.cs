using MediatR;

namespace Hmsnet.Core.Features.Iceberg.Commands;

public record RenameIcebergTableCommand(
    string FromDb,
    string FromTable,
    string ToDb,
    string ToTable)
    : IRequest;
