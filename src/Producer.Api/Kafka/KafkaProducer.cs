using System.Text.Json;
using Confluent.Kafka;
using Shared.Constants;
using Shared.Events;
using Shared.Observability;

namespace Producer.Api.Kafka;

public sealed class KafkaProducer : IKafkaProducer
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer(IProducer<string, string> producer)
    {
        _producer = producer;
    }

    public async Task PublishAsync(
        OrderCreated message,
        CancellationToken cancellationToken = default)
    {
        using var activity = Telemetry.ActivitySource.StartActivity("Kafka Publish");
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination", Topics.OrdersCreated);
        activity?.SetTag("order.id", message.OrderId);
        activity?.SetTag("customer.id", message.CustomerId);

        await _producer.ProduceAsync(
            Topics.OrdersCreated,
            new Message<string, string>
            {
                Key = message.OrderId.ToString(),
                Value = JsonSerializer.Serialize(message)
            },
            cancellationToken);
    }
}