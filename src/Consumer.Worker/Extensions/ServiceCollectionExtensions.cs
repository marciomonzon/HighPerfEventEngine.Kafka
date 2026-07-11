using Confluent.Kafka;
using Consumer.Worker.Configuration;
using Consumer.Worker.Idempotency;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Consumer.Worker.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafka(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(
            configuration.GetSection(KafkaOptions.SectionName));

        services.AddSingleton<IConsumer<string, string>>(sp =>
        {
            var options = sp
                .GetRequiredService<
                    Microsoft.Extensions.Options.IOptions<KafkaOptions>>()
                .Value;

            var config = new ConsumerConfig
            {
                BootstrapServers = options.BootstrapServers,
                GroupId = options.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            return new ConsumerBuilder<string, string>(config)
                .Build();
        });

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

        services.Configure<RedisOptions>(
            configuration.GetSection(RedisOptions.SectionName));

        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var options = sp
                .GetRequiredService<IOptions<RedisOptions>>()
                .Value;

            return ConnectionMultiplexer.Connect(options.ConnectionString);
        });

        services.AddSingleton<IProcessedMessageStore, RedisProcessedMessageStore>();

        return services;
    }
}