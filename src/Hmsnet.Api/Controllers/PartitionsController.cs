using Hmsnet.Core.DTOs;
using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Partitions.Commands;
using Hmsnet.Core.Features.Partitions.Queries;
using Hmsnet.Core.Mapping;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Api.Controllers;

[ApiController]
[Route("api/databases/{dbName}/tables/{tableName}/partitions")]
public class PartitionsController(ISender sender) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<IReadOnlyList<PartitionResponse>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetPartitions(
        string dbName, string tableName,
        [FromQuery] int maxParts = -1,
        [FromQuery] string? filter = null,
        [FromQuery] bool namesOnly = false,
        CancellationToken ct = default)
    {
        try
        {
            if (namesOnly)
            {
                var names = filter is not null
                    ? await sender.Send(new GetPartitionNamesByFilterQuery(dbName, tableName, filter, maxParts), ct)
                    : await sender.Send(new GetPartitionNamesQuery(dbName, tableName, maxParts), ct);
                return Ok(names);
            }

            var result = filter is not null
                ? await sender.Send(new GetPartitionsByFilterQuery(dbName, tableName, filter, maxParts), ct)
                : await sender.Send(new GetPartitionsQuery(dbName, tableName, maxParts), ct);

            return Ok(result.Partitions.Select(p => p.ToDto(result.PartitionKeys)));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpGet("count")]
    [ProducesResponseType<int>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetPartitionCount(string dbName, string tableName, CancellationToken ct)
    {
        try { return Ok(await sender.Send(new GetPartitionCountQuery(dbName, tableName), ct)); }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpGet("by-values")]
    [ProducesResponseType<PartitionResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetPartitionByValues(
        string dbName, string tableName,
        [FromQuery] List<string> values,
        CancellationToken ct)
    {
        try
        {
            var result = await sender.Send(new GetPartitionByValuesQuery(dbName, tableName, values), ct);
            if (result is null) return NotFound("Partition not found.");
            return Ok(result.Partition.ToDto(result.PartitionKeys));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpGet("{*partitionName}")]
    [ProducesResponseType<PartitionResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetPartitionByName(
        string dbName, string tableName, string partitionName, CancellationToken ct)
    {
        try
        {
            var result = await sender.Send(new GetPartitionByNameQuery(dbName, tableName, partitionName), ct);
            if (result is null) return NotFound("Partition not found.");
            return Ok(result.Partition.ToDto(result.PartitionKeys));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpPost]
    [ProducesResponseType<PartitionResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> AddPartition(
        string dbName, string tableName, [FromBody] PartitionRequest request, CancellationToken ct)
    {
        try
        {
            var result = await sender.Send(new AddPartitionCommand(dbName, tableName, request.ToModel()), ct);
            var partName = MetastoreMapper.BuildPartitionName(result.PartitionKeys, result.Partition.Values);
            return CreatedAtAction(nameof(GetPartitionByName),
                new { dbName, tableName, partitionName = partName },
                result.Partition.ToDto(result.PartitionKeys));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
        catch (AlreadyExistsException ex) { return Conflict(ex.Message); }
        catch (Core.Exceptions.InvalidOperationException ex) { return BadRequest(ex.Message); }
    }

    [HttpPost("batch")]
    [ProducesResponseType<IReadOnlyList<PartitionResponse>>(StatusCodes.Status200OK)]
    public async Task<IActionResult> AddPartitions(
        string dbName, string tableName, [FromBody] List<PartitionRequest> requests, CancellationToken ct)
    {
        try
        {
            var models = requests.Select(r => r.ToModel()).ToList();
            var result = await sender.Send(new AddPartitionsCommand(dbName, tableName, models), ct);
            return Ok(result.Partitions.Select(p => p.ToDto(result.PartitionKeys)));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
        catch (Core.Exceptions.InvalidOperationException ex) { return BadRequest(ex.Message); }
    }

    [HttpPut("by-values")]
    [ProducesResponseType<PartitionResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> AlterPartition(
        string dbName, string tableName, [FromBody] PartitionRequest request, CancellationToken ct)
    {
        try
        {
            var result = await sender.Send(new AlterPartitionCommand(dbName, tableName, request.ToModel()), ct);
            return Ok(result.Partition.ToDto(result.PartitionKeys));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpDelete("by-values")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> DropPartition(
        string dbName, string tableName,
        [FromQuery] List<string> values,
        [FromQuery] bool deleteData = false,
        CancellationToken ct = default)
    {
        try
        {
            var dropped = await sender.Send(new DropPartitionCommand(dbName, tableName, values, deleteData), ct);
            return dropped ? NoContent() : NotFound("Partition not found.");
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpDelete("{*partitionName}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> DropPartitionByName(
        string dbName, string tableName, string partitionName,
        [FromQuery] bool deleteData = false,
        CancellationToken ct = default)
    {
        try
        {
            var dropped = await sender.Send(new DropPartitionByNameCommand(dbName, tableName, partitionName, deleteData), ct);
            return dropped ? NoContent() : NotFound("Partition not found.");
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }
}
