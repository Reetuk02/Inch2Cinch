using System;
using System.Collections.Generic;
using System.Text;

namespace StockService.Data
{
    public class ProductUpdatedRecord
    {
        public Guid ProductId { get; set; }

        public string ProductCode { get; set; } = string.Empty;

        public string ProductName { get; set; } = string.Empty;

        public decimal Price { get; set; }

        public DateTime UpdatedAtUtc { get; set; }
    }
}
