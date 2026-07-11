using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Shared.Constants;
using Shared.Events;

namespace Consumer.Worker.Workers;

public sealed class OrderConsumerWorker : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<OrderConsumerWorker> _logger;
    private const int MaxRetries = 3;

    public OrderConsumerWorker(
        IConsumer<string, string> consumer,
        IProducer<string, string> producer,
        ILogger<OrderConsumerWorker> logger)
    {
        _consumer = consumer;
        _producer = producer;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Exception? lastException = null;

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

                        lastException = ex;
                    }
                }

                if (!processed)
                {
                    await PublishToDeadLetterAsync(
                        result,
                        lastException!,
                        stoppingToken);

                    _consumer.Commit(result);

                    _logger.LogError(
                        "Mensagem enviada para DLT");

                    continue;
                }

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

    private async Task PublishToDeadLetterAsync(
        ConsumeResult<string, string> result,
        Exception exception,
        CancellationToken cancellationToken)
    {
        var message = new Message<string, string>
        {
            Key = result.Message.Key,
            Value = result.Message.Value,
            Headers = new Headers
            {
                { "error", Encoding.UTF8.GetBytes(exception.Message) },
                { "original-topic", Encoding.UTF8.GetBytes(result.Topic) },
                { "failed-at", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("O")) }
            }
        };

        await _producer.ProduceAsync(
            Topics.OrdersCreatedDlt,
            message,
            cancellationToken);
    }
}