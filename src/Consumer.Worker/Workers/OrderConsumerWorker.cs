using System.Text.Json;
using Confluent.Kafka;
using Shared.Constants;
using Shared.Events;

namespace Consumer.Worker.Workers;

public sealed class OrderConsumerWorker : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<OrderConsumerWorker> _logger;

    public OrderConsumerWorker(
        IConsumer<string, string> consumer,
        ILogger<OrderConsumerWorker> logger)
    {
        _consumer = consumer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(Topics.OrdersCreated);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = _consumer.Consume(stoppingToken);

                var order = JsonSerializer.Deserialize<OrderCreated>(result.Message.Value);

                // Simula processamento
                await Task.Delay(1000, stoppingToken);

                _consumer.Commit(result);

                _logger.LogInformation(
                    "Committed offset {Offset}",
                    result.Offset);

                _logger.LogInformation("""
                    ========= ORDER RECEIVED =========
                    OrderId: {OrderId}
                    CustomerId: {CustomerId}
                    Amount: {Amount}
                    CreatedAt: {CreatedAt}
                    Offset: {Offset}
                    Partition: {Partition}
                    """,
                    order!.OrderId,
                    order.CustomerId,
                    order.Amount,
                    order.CreatedAt,
                    result.Offset,
                    result.Partition);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _consumer.Close();

        await Task.CompletedTask;
    }
}