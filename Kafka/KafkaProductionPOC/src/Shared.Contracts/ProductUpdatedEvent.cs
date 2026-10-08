using System;
using System.Collections.Generic;
using System.Text;

namespace Shared.Contracts
{

    public sealed record ProductUpdatedEvent(
        Guid EventId,
        Guid ProductId,
        string ProductCode,
        string ProductName,
        decimal Price,
        DateTime OccurredAtUtc);
}
