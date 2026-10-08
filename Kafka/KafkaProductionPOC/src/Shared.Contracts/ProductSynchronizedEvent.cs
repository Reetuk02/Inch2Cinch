using System;
using System.Collections.Generic;
using System.Text;

namespace Shared.Contracts
{
    public sealed record ProductSynchronizedEvent(
        Guid EventId,
        Guid ProductId,
        string ProductCode,
        DateTime SynchronizedAtUtc);
}
