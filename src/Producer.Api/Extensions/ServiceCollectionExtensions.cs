using Producer.Api.Configuration;
using Producer.Api.Kafka;

namespace Producer.Api.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafka(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(
            configuration.GetSection(KafkaOptions.SectionName));

        services.AddSingleton<IKafkaProducer, KafkaProducer>();

        return services;
    }
}