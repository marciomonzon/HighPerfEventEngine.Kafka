using Consumer.Worker.Extensions;
using Consumer.Worker.Workers;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Shared.Observability;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafka(builder.Configuration);

builder.Services.AddHostedService<OrderConsumerWorker>();

builder.Services
    .AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddAttributes(new Dictionary<string, object>
    {
        ["service.name"] = builder.Environment.ApplicationName,
        ["service.version"] = typeof(Program).Assembly.GetName().Version?.ToString() ?? "1.0.0"
    }))
    .WithTracing(tracing =>
    {
        tracing
            .SetSampler(new AlwaysOnSampler())
            .AddSource(Telemetry.ServiceName)
            .AddConsoleExporter();

        var otlpExporter = new OtlpTraceExporter(new OtlpExporterOptions
        {
            Endpoint = new Uri("http://localhost:4318"),
            Protocol = OtlpExportProtocol.HttpProtobuf
        });

        tracing.AddProcessor(new SimpleActivityExportProcessor(otlpExporter));
    });

var host = builder.Build();

host.Run();