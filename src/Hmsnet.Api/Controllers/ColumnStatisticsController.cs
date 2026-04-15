using Hmsnet.Core.DTOs;
using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.ColumnStatistics.Commands;
using Hmsnet.Core.Features.ColumnStatistics.Queries;
using Hmsnet.Core.Mapping;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Api.Controllers;

[ApiController]
public class ColumnStatisticsController(ISender sender) : ControllerBase
{
    // ── Table statistics ──────────────────────────────────────────────────────

    [HttpGet("api/databases/{dbName}/tables/{tableName}/statistics")]
    [ProducesResponseType<IReadOnlyList<ColumnStatisticsDto>>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetTableStats(
        string dbName, string tableName,
        [FromQuery] List<string> columns,
        CancellationToken ct)
    {
        try
        {
            var stats = await sender.Send(new GetTableStatsQuery(dbName, tableName, columns), ct);
            return Ok(stats.Select(s => s.ToDto()));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpPut("api/databases/{dbName}/tables/{tableName}/statistics")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> UpdateTableStats(
        string dbName, string tableName,
        [FromBody] List<ColumnStatisticsDto> statsDto,
        CancellationToken ct)
    {
        try
        {
            await sender.Send(new UpdateTableStatsCommand(dbName, tableName, statsDto.Select(s => s.ToModel())), ct);
            return NoContent();
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpDelete("api/databases/{dbName}/tables/{tableName}/statistics")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> DeleteTableStats(
        string dbName, string tableName,
        [FromQuery] string? column = null,
        CancellationToken ct = default)
    {
        try
        {
            await sender.Send(new DeleteTableStatsCommand(dbName, tableName, column), ct);
            return NoContent();
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    // ── Partition statistics ──────────────────────────────────────────────────

    [HttpGet("api/databases/{dbName}/tables/{tableName}/partitions/statistics")]
    [ProducesResponseType<IReadOnlyList<ColumnStatisticsDto>>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetPartitionStats(
        string dbName, string tableName,
        [FromQuery] List<string> values,
        [FromQuery] List<string> columns,
        CancellationToken ct)
    {
        try
        {
            var stats = await sender.Send(new GetPartitionStatsQuery(dbName, tableName, values, columns), ct);
            return Ok(stats.Select(s => s.ToDto()));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpPut("api/databases/{dbName}/tables/{tableName}/partitions/statistics")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> UpdatePartitionStats(
        string dbName, string tableName,
        [FromQuery] List<string> values,
        [FromBody] List<ColumnStatisticsDto> statsDto,
        CancellationToken ct)
    {
        try
        {
            await sender.Send(new UpdatePartitionStatsCommand(dbName, tableName, values, statsDto.Select(s => s.ToModel())), ct);
            return NoContent();
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpDelete("api/databases/{dbName}/tables/{tableName}/partitions/statistics")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> DeletePartitionStats(
        string dbName, string tableName,
        [FromQuery] List<string> values,
        [FromQuery] string? column = null,
        CancellationToken ct = default)
    {
        try
        {
            await sender.Send(new DeletePartitionStatsCommand(dbName, tableName, values, column), ct);
            return NoContent();
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }
}
