using Hmsnet.Core.DTOs;
using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Tables.Commands;
using Hmsnet.Core.Features.Tables.Queries;
using Hmsnet.Core.Mapping;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Api.Controllers;

[ApiController]
[Route("api/databases/{dbName}/tables")]
public class TablesController(ISender sender) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<IReadOnlyList<string>>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetTables(
        string dbName,
        [FromQuery] bool namesOnly = true,
        [FromQuery] string? pattern = null,
        CancellationToken ct = default)
    {
        if (namesOnly)
        {
            var names = pattern is not null
                ? await sender.Send(new GetTableNamesLikeQuery(dbName, pattern), ct)
                : await sender.Send(new GetAllTableNamesQuery(dbName), ct);
            return Ok(names);
        }

        var tables = await sender.Send(new GetAllTablesQuery(dbName), ct);
        return Ok(tables.Select(t => t.ToDto()));
    }

    [HttpGet("{tableName}")]
    [ProducesResponseType<TableResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetTable(string dbName, string tableName, CancellationToken ct)
    {
        var table = await sender.Send(new GetTableQuery(dbName, tableName), ct);
        return table is null ? NotFound($"Table '{dbName}.{tableName}' not found.") : Ok(table.ToDto());
    }

    [HttpPost("batch")]
    [ProducesResponseType<IReadOnlyList<TableResponse>>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetTablesBatch(string dbName, [FromBody] List<string> tableNames, CancellationToken ct)
    {
        var tables = await sender.Send(new GetTablesBatchQuery(dbName, tableNames), ct);
        return Ok(tables.Select(t => t.ToDto()));
    }

    [HttpPost]
    [ProducesResponseType<TableResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> CreateTable(string dbName, [FromBody] TableRequest request, CancellationToken ct)
    {
        try
        {
            var model = request.ToModel();
            model.Database = new() { Name = dbName };
            var created = await sender.Send(new CreateTableCommand(model), ct);
            return CreatedAtAction(nameof(GetTable),
                new { dbName, tableName = created.Name },
                created.ToDto());
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
        catch (AlreadyExistsException ex) { return Conflict(ex.Message); }
    }

    [HttpPut("{tableName}")]
    [ProducesResponseType<TableResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> AlterTable(string dbName, string tableName, [FromBody] TableRequest request, CancellationToken ct)
    {
        try
        {
            var updated = await sender.Send(new AlterTableCommand(dbName, tableName, request.ToModel()), ct);
            return Ok(updated.ToDto());
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpDelete("{tableName}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> DropTable(
        string dbName, string tableName,
        [FromQuery] bool deleteData = false, CancellationToken ct = default)
    {
        try
        {
            await sender.Send(new DropTableCommand(dbName, tableName, deleteData), ct);
            return NoContent();
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpGet("{tableName}/exists")]
    [ProducesResponseType<bool>(StatusCodes.Status200OK)]
    public async Task<IActionResult> TableExists(string dbName, string tableName, CancellationToken ct) =>
        Ok(await sender.Send(new TableExistsQuery(dbName, tableName), ct));

    [HttpGet("{tableName}/fields")]
    [ProducesResponseType<IReadOnlyList<ColumnDto>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetFields(string dbName, string tableName, CancellationToken ct)
    {
        try
        {
            var cols = await sender.Send(new GetFieldsQuery(dbName, tableName), ct);
            return Ok(cols.Select(c => c.ToDto()));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpGet("{tableName}/schema")]
    [ProducesResponseType<IReadOnlyList<ColumnDto>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetSchema(string dbName, string tableName, CancellationToken ct)
    {
        try
        {
            var cols = await sender.Send(new GetSchemaQuery(dbName, tableName), ct);
            return Ok(cols.Select(c => c.ToDto()));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }
}
