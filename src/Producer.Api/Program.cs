using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Producer.Api.Contracts;
using Producer.Api.Extensions;
using Producer.Api.Kafka;
using Shared.Events;
using Shared.Observability;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKafka(builder.Configuration);

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
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddConsoleExporter();

        var otlpExporter = new OtlpTraceExporter(new OtlpExporterOptions
        {
            Endpoint = new Uri("http://localhost:4318"),
            Protocol = OtlpExportProtocol.HttpProtobuf
        });

        tracing.AddProcessor(new SimpleActivityExportProcessor(otlpExporter));
    });

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
}

app.MapPost("/orders", async (
    CreateOrderRequest request,
    IKafkaProducer producer,
    CancellationToken cancellationToken) =>
{
    var order = new OrderCreated(
        request.OrderId,
        request.CustomerId,
        request.Amount,
        DateTime.UtcNow);

    await producer.PublishAsync(order, cancellationToken);

    return Results.Accepted(null, order);
});

app.Run();