using StackExchange.Redis;

namespace Consumer.Worker.Idempotency;

public sealed class RedisProcessedMessageStore
    : IProcessedMessageStore
{
    private readonly IDatabase _database;

    public RedisProcessedMessageStore(
        IConnectionMultiplexer redis)
    {
        _database = redis.GetDatabase();
    }

    public Task<bool> HasProcessedAsync(Guid messageId)
    {
        return _database.KeyExistsAsync(GetKey(messageId));
    }

    public Task MarkAsProcessedAsync(Guid messageId)
    {
        return _database.StringSetAsync(
            GetKey(messageId),
            "1",
            TimeSpan.FromDays(1));
    }

    private static string GetKey(Guid id)
        => $"processed:{id}";
}