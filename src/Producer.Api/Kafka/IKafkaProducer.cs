using Shared.Events;

namespace Producer.Api.Kafka;

public interface IKafkaProducer
{
    Task PublishAsync(OrderCreated message, CancellationToken cancellationToken = default);
}