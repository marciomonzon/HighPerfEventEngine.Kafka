using Producer.Api.Contracts;
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
    CreateOrderRequest request,
    IKafkaProducer producer,
    CancellationToken cancellationToken) =>
{
    var order = new OrderCreated(
        Guid.NewGuid(),
        request.CustomerId,
        request.Amount,
        DateTime.UtcNow);

    await producer.PublishAsync(order, cancellationToken);

    return Results.Accepted(null, order);
});

app.Run();