namespace Producer.Api.Contracts;

public sealed record CreateOrderRequest(
    Guid CustomerId,
    decimal Amount);