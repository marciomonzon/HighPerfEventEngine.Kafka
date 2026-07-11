using System.Diagnostics;

namespace Shared.Observability;

public static class Telemetry
{
    public const string ServiceName = "HighPerfEventEngine.Kafka";

    public static readonly ActivitySource ActivitySource =
        new(ServiceName);
}