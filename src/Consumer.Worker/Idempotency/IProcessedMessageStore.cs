namespace Consumer.Worker.Idempotency
{
    public interface IProcessedMessageStore
    {
        Task<bool> HasProcessedAsync(Guid messageId);
        Task MarkAsProcessedAsync(Guid messageId);
    }
}