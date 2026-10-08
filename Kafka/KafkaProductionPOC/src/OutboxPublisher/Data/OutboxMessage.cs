using System;
using System.Collections.Generic;
using System.Text;

namespace OutboxPublisher.Data
{
    public class OutboxMessage
    {
        public Guid Id { get; set; }

        public string EventType { get; set; } = string.Empty;

        public string Payload { get; set; } = string.Empty;

        public DateTime CreatedAtUtc { get; set; }

        public bool Published { get; set; }

        public DateTime? PublishedAtUtc { get; set; }
    }
}
