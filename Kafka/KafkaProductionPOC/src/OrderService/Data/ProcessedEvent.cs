using System;
using System.Collections.Generic;
using System.Text;

namespace OrderService.Data
{
    public class ProcessedEvent
    {
        public Guid EventId { get; set; }

        public DateTime ProcessedAtUtc { get; set; }
    }
}
