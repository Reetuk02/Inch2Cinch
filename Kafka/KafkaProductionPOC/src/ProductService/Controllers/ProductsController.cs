using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ProductService.Data;
using Shared.Contracts;

namespace ProductService.Controllers
{
    [ApiController]
    [Route("products")]
    public class ProductsController : ControllerBase
    {
        private readonly ProductDbContext _db;
        public ProductsController(ProductDbContext db)
        {
            _db = db;
        }
        [HttpPost]
        public async Task<IActionResult> Create(
        CreateProductRequest request,
        CancellationToken cancellationToken)
        {
            var product = new Product
            {
                Id = Guid.NewGuid(),
                ProductCode = request.ProductCode,
                Name = request.Name,
                Price = request.Price,
                UpdatedAtUtc = DateTime.UtcNow
            };
            var integrationEvent = new ProductUpdatedEvent(
            Guid.NewGuid(),
            product.Id,
            product.ProductCode,
            product.Name,
            product.Price,
            product.UpdatedAtUtc);
            var outbox = new OutboxMessage
            {
                Id = integrationEvent.EventId,
                EventType = nameof(ProductUpdatedEvent),
                Payload = JsonSerializer.Serialize(integrationEvent),
                CreatedAtUtc = DateTime.UtcNow,
                Published = false
            };
            await using var transaction =
            await _db.Database.BeginTransactionAsync(
                cancellationToken);
            _db.Products.Add(product);
            _db.OutboxMessages.Add(outbox);
            await _db.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            return Ok(new
            {
                product.Id,
                integrationEvent.EventId
            });

        }
    }
}
    