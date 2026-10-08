using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OutboxPublisher.Data;
namespace OutboxPublisher
{
    public class Worker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        public Worker( IServiceScopeFactory scopeFactory, ILogger<Worker> logger,IConfiguration configuration) {
            _scopeFactory = scopeFactory;
            _logger = logger;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers =
               _configuration["Kafka:BootstrapServers"],

                EnableIdempotence = true,
                Acks = Acks.All
            };
           using var producer =
           new ProducerBuilder<string, string>(
               producerConfig)
           .Build();


            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var dbContext = scope.ServiceProvider.GetRequiredService<OutboxDbContext>();
                    // fetch messages from the outbox table that have not been published yet
                    var messages = await dbContext.OutboxMessages
                        .Where(x => !x.Published)
                        .OrderBy(x => x.CreatedAtUtc)
                        .Take(10)
                        .ToListAsync(stoppingToken);
                    foreach (var message in messages)
                    {
                        // publish the message to Kafka
                        var result = await producer.ProduceAsync(
                         "ProductUpdated",
                         new Message<string, string>
                         {
                             Key = message.Id.ToString(),
                             Value = message.Payload
                         },
                         stoppingToken);

                        _logger.LogInformation("Published EventId {EventId} to {Topic}-{Partition}@{Offset}",
                          message.Id,
                          result.Topic,
                          result.Partition,
                          result.Offset);

                        message.Published = true;
                        message.PublishedAtUtc = DateTime.UtcNow;
                    }
                    // save the changes to the outbox table to mark the messages as published
                    await dbContext.SaveChangesAsync(stoppingToken);
                    await Task.Delay(
                                  TimeSpan.FromSeconds(1),
                                  stoppingToken);

                }
                catch(Exception ex) {
                
                    _logger.LogError(
                    ex,
                    "Outbox publisher failed.");

                    await Task.Delay(
                        TimeSpan.FromSeconds(5),
                        stoppingToken);
                }
            }
        }
    }
}
