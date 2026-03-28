using Hmsnet.Core.DTOs;
using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Mapping;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Api.Controllers;

[ApiController]
[Route("api/databases/{dbName}/tables/{tableName}/partitions")]
public class PartitionsController(IPartitionService svc, ITableService tableSvc) : ControllerBase
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
            var partKeys = (await tableSvc.GetSchemaAsync(dbName, tableName, ct))
                .Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition).ToList();

            if (namesOnly)
            {
                var names = filter is not null
                    ? await svc.GetPartitionNamesByFilterAsync(dbName, tableName, filter, maxParts, ct)
                    : await svc.GetPartitionNamesAsync(dbName, tableName, maxParts, ct);
                return Ok(names);
            }

            var partitions = filter is not null
                ? await svc.GetPartitionsByFilterAsync(dbName, tableName, filter, maxParts, ct)
                : await svc.GetPartitionsAsync(dbName, tableName, maxParts, ct);

            var pKeys = partKeys.Select(c => new Hmsnet.Core.Models.HiveColumn
            {
                Name = c.Name, TypeName = c.TypeName, OrdinalPosition = c.OrdinalPosition, IsPartitionKey = true
            }).ToList();

            return Ok(partitions.Select(p => p.ToDto(pKeys)));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpGet("count")]
    [ProducesResponseType<int>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetPartitionCount(string dbName, string tableName, CancellationToken ct)
    {
        try { return Ok(await svc.GetPartitionCountAsync(dbName, tableName, ct)); }
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
            var p = await svc.GetPartitionAsync(dbName, tableName, values, ct);
            if (p is null) return NotFound("Partition not found.");

            var partKeys = (await tableSvc.GetSchemaAsync(dbName, tableName, ct))
                .Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition)
                .Select(c => new Hmsnet.Core.Models.HiveColumn { Name = c.Name, OrdinalPosition = c.OrdinalPosition })
                .ToList();
            return Ok(p.ToDto(partKeys));
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
            var p = await svc.GetPartitionByNameAsync(dbName, tableName, partitionName, ct);
            if (p is null) return NotFound("Partition not found.");

            var partKeys = (await tableSvc.GetSchemaAsync(dbName, tableName, ct))
                .Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition)
                .Select(c => new Hmsnet.Core.Models.HiveColumn { Name = c.Name, OrdinalPosition = c.OrdinalPosition })
                .ToList();
            return Ok(p.ToDto(partKeys));
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
            var p = await svc.AddPartitionAsync(dbName, tableName, request.ToModel(), ct);
            var partKeys = (await tableSvc.GetSchemaAsync(dbName, tableName, ct))
                .Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition)
                .Select(c => new Hmsnet.Core.Models.HiveColumn { Name = c.Name, OrdinalPosition = c.OrdinalPosition })
                .ToList();
            var partName = MetastoreMapper.BuildPartitionName(partKeys, p.Values);
            return CreatedAtAction(nameof(GetPartitionByName),
                new { dbName, tableName, partitionName = partName },
                p.ToDto(partKeys));
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
            var partitions = await svc.AddPartitionsAsync(dbName, tableName, models, ct);
            var partKeys = (await tableSvc.GetSchemaAsync(dbName, tableName, ct))
                .Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition)
                .Select(c => new Hmsnet.Core.Models.HiveColumn { Name = c.Name, OrdinalPosition = c.OrdinalPosition })
                .ToList();
            return Ok(partitions.Select(p => p.ToDto(partKeys)));
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
            var updated = await svc.AlterPartitionAsync(dbName, tableName, request.ToModel(), ct);
            var partKeys = (await tableSvc.GetSchemaAsync(dbName, tableName, ct))
                .Where(c => c.IsPartitionKey).OrderBy(c => c.OrdinalPosition)
                .Select(c => new Hmsnet.Core.Models.HiveColumn { Name = c.Name, OrdinalPosition = c.OrdinalPosition })
                .ToList();
            return Ok(updated.ToDto(partKeys));
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
            var dropped = await svc.DropPartitionAsync(dbName, tableName, values, deleteData, ct);
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
            var dropped = await svc.DropPartitionByNameAsync(dbName, tableName, partitionName, deleteData, ct);
            return dropped ? NoContent() : NotFound("Partition not found.");
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }
}
