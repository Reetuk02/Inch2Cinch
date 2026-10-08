using System;
using System.Collections.Generic;
using System.Text;

namespace OrderService.Data
{
    public class ProductSynchronization
    {
        public Guid ProductId { get; set; }

        public string ProductCode { get; set; } = string.Empty;

        public decimal Price { get; set; }

        public DateTime SynchronizedAtUtc { get; set; }
    }
}
