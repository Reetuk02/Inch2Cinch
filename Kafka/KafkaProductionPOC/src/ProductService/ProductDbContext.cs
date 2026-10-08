using Microsoft.EntityFrameworkCore;
using ProductService.Data;
namespace ProductService
{
    public class ProductDbContext : DbContext
    {
        public ProductDbContext(
            DbContextOptions<ProductDbContext> options)
            : base(options)
        {
        }

        public DbSet<Product> Products => Set<Product>();

        public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

        protected override void OnModelCreating(
            ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Product>()
                .HasKey(x => x.Id);

            modelBuilder.Entity<Product>()
                .HasIndex(x => x.ProductCode)
                .IsUnique();

            modelBuilder.Entity<OutboxMessage>()
                .HasKey(x => x.Id);

            modelBuilder.Entity<OutboxMessage>()
                .HasIndex(x => new
                {
                    x.Published,
                    x.CreatedAtUtc
                });
        }
    }
}
