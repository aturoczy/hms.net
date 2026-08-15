using Hmsnet.Core.DTOs;
using Hmsnet.Core.Exceptions;
using Hmsnet.Core.Features.Catalogs.Commands;
using Hmsnet.Core.Features.Catalogs.Queries;
using Hmsnet.Core.Models;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Api.Controllers;

[ApiController]
[Route("api/catalogs")]
public class CatalogsController(ISender sender) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<IReadOnlyList<string>>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetAllCatalogs(
        [FromQuery] bool namesOnly = true, CancellationToken ct = default)
    {
        if (namesOnly)
            return Ok(await sender.Send(new GetAllCatalogNamesQuery(), ct));

        var cats = await sender.Send(new GetAllCatalogsQuery(), ct);
        return Ok(cats.Select(ToDto));
    }

    [HttpGet("{name}")]
    [ProducesResponseType<CatalogResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetCatalog(string name, CancellationToken ct)
    {
        var cat = await sender.Send(new GetCatalogQuery(name), ct);
        return cat is null ? NotFound($"Catalog '{name}' not found.") : Ok(ToDto(cat));
    }

    [HttpGet("{name}/exists")]
    [ProducesResponseType<bool>(StatusCodes.Status200OK)]
    public async Task<IActionResult> CatalogExists(string name, CancellationToken ct) =>
        Ok(await sender.Send(new CatalogExistsQuery(name), ct));

    [HttpPost]
    [ProducesResponseType<CatalogResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> CreateCatalog([FromBody] CatalogRequest request, CancellationToken ct)
    {
        try
        {
            var cat = await sender.Send(new CreateCatalogCommand(ToModel(request)), ct);
            return CreatedAtAction(nameof(GetCatalog), new { name = cat.Name }, ToDto(cat));
        }
        catch (AlreadyExistsException ex) { return Conflict(ex.Message); }
    }

    [HttpPut("{name}")]
    [ProducesResponseType<CatalogResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> AlterCatalog(string name, [FromBody] CatalogRequest request, CancellationToken ct)
    {
        try
        {
            var cat = await sender.Send(new AlterCatalogCommand(name, ToModel(request)), ct);
            return Ok(ToDto(cat));
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
    }

    [HttpDelete("{name}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> DropCatalog(string name, CancellationToken ct)
    {
        try
        {
            await sender.Send(new DropCatalogCommand(name), ct);
            return NoContent();
        }
        catch (NoSuchObjectException ex) { return NotFound(ex.Message); }
        catch (Core.Exceptions.InvalidOperationException ex) { return Conflict(ex.Message); }
    }

    private static Catalog ToModel(CatalogRequest r) => new()
    {
        Name = r.Name,
        Description = r.Description,
        LocationUri = r.LocationUri,
        Properties = r.Properties ?? []
    };

    private static CatalogResponse ToDto(Catalog c) =>
        new(c.Name, c.Description, c.LocationUri, c.Properties, c.CreateTime);
}
