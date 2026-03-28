using Hmsnet.Core.DTOs;
using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Interfaces;
using Hmsnet.Core.Mapping;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Api.Controllers;

[ApiController]
[Route("api/databases")]
public class DatabasesController(IDatabaseService svc) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<IReadOnlyList<string>>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetAllDatabases(
        [FromQuery] bool namesOnly = true, CancellationToken ct = default)
    {
        if (namesOnly)
            return Ok(await svc.GetAllDatabaseNamesAsync(ct));

        var dbs = await svc.GetAllDatabasesAsync(ct);
        return Ok(dbs.Select(d => d.ToDto()));
    }

    [HttpGet("{name}")]
    [ProducesResponseType<DatabaseResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetDatabase(string name, CancellationToken ct)
    {
        var db = await svc.GetDatabaseAsync(name, ct);
        return db is null ? NotFound($"Database '{name}' not found.") : Ok(db.ToDto());
    }

    [HttpPost]
    [ProducesResponseType<DatabaseResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> CreateDatabase([FromBody] DatabaseRequest request, CancellationToken ct)
    {
        try
        {
            var db = await svc.CreateDatabaseAsync(request.ToModel(), ct);
            return CreatedAtAction(nameof(GetDatabase), new { name = db.Name }, db.ToDto());
        }
        catch (AlreadyExistsException ex)
        {
            return Conflict(ex.Message);
        }
    }

    [HttpPut("{name}")]
    [ProducesResponseType<DatabaseResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> AlterDatabase(string name, [FromBody] DatabaseRequest request, CancellationToken ct)
    {
        try
        {
            var db = await svc.AlterDatabaseAsync(name, request.ToModel(), ct);
            return Ok(db.ToDto());
        }
        catch (NoSuchObjectException ex)
        {
            return NotFound(ex.Message);
        }
    }

    [HttpDelete("{name}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> DropDatabase(
        string name, [FromQuery] bool cascade = false, CancellationToken ct = default)
    {
        try
        {
            await svc.DropDatabaseAsync(name, cascade, ct);
            return NoContent();
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
        catch (Core.Exceptions.InvalidOperationException ex) { return Conflict(ex.Message); }
    }

    [HttpGet("{name}/exists")]
    [ProducesResponseType<bool>(StatusCodes.Status200OK)]
    public async Task<IActionResult> DatabaseExists(string name, CancellationToken ct) =>
        Ok(await svc.DatabaseExistsAsync(name, ct));
}
