using System;
using System.Collections.Generic;
using System.Text;

namespace StockService.Data
{
    public class ProcessedEvent
    {
        public Guid EventId { get; set; }

        public DateTime ProcessedAtUtc { get; set; }
    }
}
