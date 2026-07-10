using Producer.Api.Extensions;
using Producer.Api.Kafka;
using Shared.Events;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddKafka(builder.Configuration);

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
}

app.MapPost("/orders", async (
    IKafkaProducer producer,
    CancellationToken cancellationToken) =>
{
    var order = new OrderCreated(
        Guid.NewGuid(),
        Guid.NewGuid(),
        Random.Shared.Next(100, 5000),
        DateTime.UtcNow);

    await producer.PublishAsync(order, cancellationToken);

    return Results.Accepted();
});

app.Run();