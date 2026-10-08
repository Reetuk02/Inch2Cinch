using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using System;
using System.Collections.Generic;
using System.Text;

namespace OrderService
{
    public class OrderDbContext : DbContext
    {
        public OrderDbContext(
        DbContextOptions<OrderDbContext> options)
        : base(options)
        {
        }
        public DbSet<ProcessedEvent> ProcessedEvents =>Set<ProcessedEvent>();

        public DbSet<ProductSynchronization> ProductSynchronizations => Set<ProductSynchronization>();


        protected override void OnModelCreating(
            ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ProcessedEvent>()
                .HasKey(x => x.EventId);

            modelBuilder.Entity<ProductSynchronization>()
                .HasKey(x => x.ProductId);
        }
    }
}
