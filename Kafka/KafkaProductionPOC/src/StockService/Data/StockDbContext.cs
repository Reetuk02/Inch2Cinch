using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Text;

namespace StockService.Data
{
    public class StockDbContext : DbContext
    {
        public StockDbContext(
        DbContextOptions<StockDbContext> options)
        : base(options)
        {
        }
        public DbSet<ProcessedEvent> ProcessedEvents =>
      Set<ProcessedEvent>();

        public DbSet<ProductUpdatedRecord> ProductUpdatedRecords =>
            Set<ProductUpdatedRecord>();

        protected override void OnModelCreating(
        ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ProcessedEvent>()
                .HasKey(x => x.EventId);

            modelBuilder.Entity<ProductUpdatedRecord>()
                .HasKey(x => x.ProductId);
        }
    }
}
