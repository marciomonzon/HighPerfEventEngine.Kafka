using Confluent.Kafka;
using Consumer.Worker.Configuration;

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
                EnableAutoCommit = true
            };

            return new ConsumerBuilder<string, string>(config)
                .Build();
        });

        return services;
    }
}