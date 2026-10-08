using Confluent.Kafka;
using Shared.Contracts;
using System.Text.Json;
using StockService.Data;
using Microsoft.EntityFrameworkCore;
namespace StockService
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
        protected override async Task ExecuteAsync(
            CancellationToken stoppingToken)
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
        })

        .SetPartitionsAssignedHandler(
            (_, partitions) =>
            {
                _logger.LogInformation(
                    "MyGTS assigned partitions: {Partitions}",
                    string.Join(
                        ", ",
                        partitions));
            })

        .SetPartitionsRevokedHandler(
            (_, partitions) =>
            {
                _logger.LogInformation(
                    "MyGTS revoked partitions: {Partitions}",
                    string.Join(
                        ", ",
                        partitions));
            }).Build();

            consumer.Subscribe(_configuration["Kafka:Topic"]);
            _logger.LogInformation(
          "MyGTS started. GroupId = {GroupId}, Topic = {Topic}",
          _configuration["Kafka:GroupId"],
          _configuration["Kafka:Topic"]);
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result =
                 consumer.Consume(stoppingToken);
                    try
                    {
                        var message = JsonSerializer.Deserialize<ProductUpdatedEvent>(result.Message.Value);
                        if (message == null)
                        {
                            throw new InvalidOperationException(
                                "Unable to deserialize ProductUpdatedEvent.");
                        }

                        await ProcessMessageAsync(
                            message,
                            result,
                            stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            ex,
                            "MyGTS failed to process message at " +
                            "{Topic}-{Partition}@{Offset}. " +
                            "Offset will NOT be committed.",
                            result.Topic,
                            result.Partition,
                            result.Offset);

                        await Task.Delay(
                            TimeSpan.FromSeconds(2),
                            stoppingToken);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "MyGTS error: {Message}",
                    ex.Message);
            }
            finally
            {
                consumer.Close();
            }
        }
        private async Task ProcessMessageAsync(
        ProductUpdatedEvent message,
        ConsumeResult<Ignore, string> kafkaRecord,
        CancellationToken cancellationToken)
        {
            
                using var scope =
                    _scopeFactory.CreateScope();

            var db = scope.ServiceProvider
             .GetRequiredService<StockDbContext>();

            await using var transaction =
                await db.Database.BeginTransactionAsync(
                    cancellationToken);

            var alreadyProcessed =
                await db.ProcessedEvents
                    .AnyAsync(
                        x => x.EventId == message.EventId,
                        cancellationToken);
            if (alreadyProcessed)
            {
                _logger.LogWarning(
                    "MyGTS received duplicate EventId {EventId}. " +
                    "Business processing will be skipped.",
                    message.EventId);

                await transaction.CommitAsync(
                    cancellationToken);

                return;
            }

            var existingProduct =
                await db.ProductUpdatedRecords
                    .FirstOrDefaultAsync(
                        x => x.ProductId == message.ProductId,
                        cancellationToken);

            if (existingProduct == null)
            {
                db.ProductUpdatedRecords.Add(
                    new ProductUpdatedRecord
                    {
                        ProductId = message.ProductId,
                        ProductCode = message.ProductCode,
                        ProductName = message.ProductName,
                        Price = message.Price,
                        UpdatedAtUtc = message.OccurredAtUtc
                    });
            }
            else
            {
                existingProduct.ProductCode =
                    message.ProductCode;

                existingProduct.ProductName =
                    message.ProductName;

                existingProduct.Price =
                    message.Price;

                existingProduct.UpdatedAtUtc =
                    message.OccurredAtUtc;
            }
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

            _logger.LogInformation(
                "Stock database processing completed for " +
                "EventId {EventId}",
                message.EventId);

        }
    }
}


