namespace Shared.Events
{
    public sealed record OrderCreated(Guid OrderId,
                                      Guid CustomerId,
                                      decimal Amount,
                                      DateTime CreatedAt);
}