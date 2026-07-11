namespace Producer.Api.Contracts;

public sealed record CreateOrderRequest(
    Guid OrderId,
    Guid CustomerId,
    decimal Amount);