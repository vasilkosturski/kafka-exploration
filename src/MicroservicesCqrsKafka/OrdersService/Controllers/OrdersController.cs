using Microsoft.AspNetCore.Mvc;

namespace OrdersService.Controllers;

[ApiController]
[Route("[controller]")]
public class OrdersController : ControllerBase
{
    [HttpGet]
    public ActionResult Get() => Ok("hello");
}