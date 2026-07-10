using System.Text.Json;
using Confluent.Kafka;
using Shared.Constants;
using Shared.Events;

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