using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Consumer.Worker.Idempotency;
using Microsoft.Extensions.Hosting;
using Shared.Constants;
using Shared.Events;

namespace Consumer.Worker.Workers;

public sealed class OrderConsumerWorker : BackgroundService
{
    private const int MaxRetries = 3;

    private readonly IConsumer<string, string> _consumer;
    private readonly IProducer<string, string> _producer;
    private readonly IProcessedMessageStore _store;
    private readonly ILogger<OrderConsumerWorker> _logger;

    public OrderConsumerWorker(
        IConsumer<string, string> consumer,
        IProducer<string, string> producer,
        IProcessedMessageStore store,
        ILogger<OrderConsumerWorker> logger)
    {
        _consumer = consumer;
        _producer = producer;
        _store = store;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(Topics.OrdersCreated);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = _consumer.Consume(stoppingToken);

                    await ProcessMessageAsync(result, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Erro ao consumir mensagem.");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro inesperado.");
                }
            }
        }
        finally
        {
            _consumer.Close();
            _consumer.Dispose();
        }
    }

    private async Task ProcessMessageAsync(
        ConsumeResult<string, string> result,
        CancellationToken cancellationToken)
    {
        var order = JsonSerializer.Deserialize<OrderCreated>(result.Message.Value);

        if (order is null)
        {
            _logger.LogWarning("Mensagem inválida.");

            _consumer.Commit(result);

            return;
        }

        var orederTest = new Guid("1cb45a19-0eb6-496d-9014-cd658d5e68a3");
        if (await _store.HasProcessedAsync(order.OrderId))
        {
            _logger.LogInformation(
                "Pedido {OrderId} já foi processado.",
                order.OrderId);

            _consumer.Commit(result);

            return;
        }

        Exception? lastException = null;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            try
            {
                await ProcessOrderAsync(order, cancellationToken);

                await _store.MarkAsProcessedAsync(order.OrderId);

                _consumer.Commit(result);

                _logger.LogInformation(
                    "Pedido {OrderId} processado com sucesso. Offset {Offset} confirmado.",
                    order.OrderId,
                    result.Offset);

                return;
            }
            catch (Exception ex)
            {
                lastException = ex;

                _logger.LogWarning(
                    ex,
                    "Tentativa {Attempt}/{MaxRetries} para o pedido {OrderId}.",
                    attempt,
                    MaxRetries,
                    order.OrderId);

                if (attempt < MaxRetries)
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(attempt),
                        cancellationToken);
                }
            }
        }

        await PublishToDeadLetterAsync(
            result,
            lastException!,
            cancellationToken);

        _consumer.Commit(result);

        _logger.LogError(
            lastException,
            "Pedido {OrderId} enviado para a Dead Letter Topic.",
            order.OrderId);
    }

    private async Task ProcessOrderAsync(
        OrderCreated order,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Processando pedido {OrderId}.",
            order.OrderId);

        await Task.Delay(1000, cancellationToken);

        // Simulação
        //throw new Exception("Erro no processamento");
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