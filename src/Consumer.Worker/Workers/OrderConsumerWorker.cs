using System.Text.Json;
using Confluent.Kafka;
using Shared.Constants;
using Shared.Events;

namespace Consumer.Worker.Workers;

public sealed class OrderConsumerWorker : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly ILogger<OrderConsumerWorker> _logger;

    private const int MaxRetries = 3;

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

        var order = JsonSerializer.Deserialize<OrderCreated>(
            result.Message.Value)!;

        var processed = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            try
            {
                await ProcessOrderAsync(order, stoppingToken);

                processed = true;

                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Tentativa {Attempt}/{MaxRetries}",
                    attempt,
                    MaxRetries);

                if (attempt < MaxRetries)
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(attempt),
                        stoppingToken);
                }
            }
        }

        if (!processed)
        {
            _logger.LogError(
                "Falha definitiva no pedido {OrderId}",
                order.OrderId);

            continue;
        }

        _consumer.Commit(result);

        _logger.LogInformation(
            "Offset {Offset} confirmado",
            result.Offset);
    }
    catch (OperationCanceledException)
    {
        break;
    }
}

        _consumer.Close();

        await Task.CompletedTask;
    }

    private async Task ProcessOrderAsync(
    OrderCreated order,
    CancellationToken cancellationToken)
{
    _logger.LogInformation(
        "Processando pedido {OrderId}",
        order.OrderId);

    await Task.Delay(1000, cancellationToken);

    // Simular erro
    throw new Exception("Erro no processamento");
}
}