using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore;
namespace OutboxPublisher.Data
{
    public class OutboxDbContext : DbContext
    {
        public OutboxDbContext( DbContextOptions<OutboxDbContext> options) : base(options)
        {
        }
        public DbSet<OutboxMessage> OutboxMessages =>Set<OutboxMessage>();


    }
}
