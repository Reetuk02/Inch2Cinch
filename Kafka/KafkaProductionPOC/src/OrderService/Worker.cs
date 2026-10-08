using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using OrderService.Data;
using Shared.Contracts;
using System.Text.Json;
using System.Text.RegularExpressions;
namespace OrderService
{
    public class Worker : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IConfiguration _configuration;
        private readonly ILogger<Worker> _logger;
        public Worker(
              IServiceScopeFactory scopeFactory,
              IConfiguration configuration,
              ILogger<Worker> logger)
        {
            _scopeFactory = scopeFactory;
            _configuration = configuration;
            _logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers =
             _configuration["Kafka:BootstrapServers"],

                GroupId =
             _configuration["Kafka:GroupId"],

                AutoOffsetReset =
             AutoOffsetReset.Earliest,

                EnableAutoCommit = false,

                EnableAutoOffsetStore = false
            };
            using var consumer =
           new ConsumerBuilder<Ignore, string>(config)
               .SetErrorHandler((_, error) =>
               {
                   _logger.LogError(
                       "Kafka error: {Reason}",
                       error.Reason);
               }).SetPartitionsAssignedHandler(
                    (_, partitions) =>
                    {
                        _logger.LogInformation(
                            "Assigned partitions: {Partitions}",
                            string.Join(", ", partitions));
                    })
                .SetPartitionsRevokedHandler(
                    (_, partitions) =>
                    {
                        _logger.LogInformation(
                            "Revoked partitions: {Partitions}",
                            string.Join(", ", partitions));
                    }).Build();
            consumer.Subscribe(
                  _configuration["Kafka:Topic"]);
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result =
                  consumer.Consume(stoppingToken);
                    try
                    {
                        var message =
                            JsonSerializer.Deserialize<ProductUpdatedEvent>(
                                result.Message.Value)
                            ?? throw new InvalidOperationException(
                                "Invalid ProductUpdated payload.");

                        await ProcessMessageAsync(
                            message,
                            stoppingToken);

                        consumer.StoreOffset(result);

                        consumer.Commit(result);

                        _logger.LogInformation(
                            "Processed EventId {EventId}, Offset {Offset}",
                            message.EventId,
                            result.Offset);
                    }


                    catch (Exception ex) { }
                    finally
                    {
                        consumer.Close();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation(
                    "Worker is stopping due to cancellation.");
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "An error occurred in the worker.");
            }
        }
        private async Task ProcessMessageAsync(ProductUpdatedEvent message, CancellationToken cancellationToken)
        {
            using var scope =
            _scopeFactory.CreateScope();
            var db = scope.ServiceProvider
              .GetRequiredService<OrderDbContext>();
            await using var transaction =
            await db.Database.BeginTransactionAsync( cancellationToken);
            var alreadyProcessed =
            await db.ProcessedEvents.AnyAsync(
              x => x.EventId == message.EventId,
              cancellationToken);

            if (alreadyProcessed)
            {
                _logger.LogWarning(
                    "Duplicate EventId {EventId}. Skipping business processing.",
                    message.EventId);

                await transaction.CommitAsync(
                    cancellationToken);

                return;
            }

            db.ProductSynchronizations.Add(
                new ProductSynchronization
                {
                    ProductId = message.ProductId,
                    ProductCode = message.ProductCode,
                    Price = message.Price,
                    SynchronizedAtUtc = DateTime.UtcNow
                });
            db.ProcessedEvents.Add(
                      new ProcessedEvent
                      {
                          EventId = message.EventId,
                          ProcessedAtUtc = DateTime.UtcNow
                      });
            await db.SaveChangesAsync(
                       cancellationToken);

            await transaction.CommitAsync(
                cancellationToken);
        }
    } 
}
