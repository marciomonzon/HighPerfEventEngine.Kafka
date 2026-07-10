using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Producer.Api.Configuration;
using Shared.Constants;
using Shared.Events;

namespace Producer.Api.Kafka;

public sealed class KafkaProducer : IKafkaProducer
{
    private readonly IProducer<string, string> _producer;

    public KafkaProducer(IOptions<KafkaOptions> options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishAsync(
        OrderCreated message,
        CancellationToken cancellationToken = default)
    {
        var kafkaMessage = new Message<string, string>
        {
            Key = message.OrderId.ToString(),
            Value = JsonSerializer.Serialize(message)
        };

        await _producer.ProduceAsync(
            Topics.OrdersCreated,
            kafkaMessage,
            cancellationToken);
    }
}