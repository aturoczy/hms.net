using Microsoft.AspNetCore.Mvc;

namespace Hmsnet.Iceberg.Controllers;

[ApiController]
[Route("v1")]
public class CatalogConfigController : ControllerBase
{
    /// <summary>Returns catalog configuration. Clients use this to discover supported features.</summary>
    [HttpGet("config")]
    public IActionResult GetConfig([FromQuery] string? warehouse = null) =>
        Ok(new
        {
            defaults = new Dictionary<string, string>(),
            overrides = new Dictionary<string, string>()
        });
}
