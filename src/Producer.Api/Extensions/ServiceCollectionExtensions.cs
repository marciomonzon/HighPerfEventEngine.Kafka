using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Producer.Api.Configuration;
using Producer.Api.Kafka;

namespace Producer.Api.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafka(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(
            configuration.GetSection(KafkaOptions.SectionName));

        services.AddSingleton<IProducer<string, string>>(sp =>
        {
            var options = sp
                .GetRequiredService<IOptions<KafkaOptions>>()
                .Value;

            var config = new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers
            };

            return new ProducerBuilder<string, string>(config)
                .Build();
        });

        services.AddSingleton<IKafkaProducer, KafkaProducer>();

        return services;
    }
}