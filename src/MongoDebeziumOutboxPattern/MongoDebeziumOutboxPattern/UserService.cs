using System.Text.Json;
using MongoDB.Driver;

namespace MongoDebeziumOutboxPattern;

public class UserService
{
    private readonly IMongoCollection<User> _usersCollection;
    private readonly IMongoCollection<OutboxRecord> _outboxCollection;
    private readonly MongoClient _client;

    public UserService()
    {
        _client = new MongoClient("mongodb://localhost:27017");
        var database = _client.GetDatabase("testdb");
        _usersCollection = database.GetCollection<User>("users");
        _outboxCollection = database.GetCollection<OutboxRecord>("outbox");
    }

    public async Task CreateUserAsync()
    {
        using var session = await _client.StartSessionAsync();
        session.StartTransaction();

        try
        {
            var newUser = new User
            {
                Name = "Adam"
            };
            await _usersCollection.InsertOneAsync(session, newUser);

            var outboxRecord = new OutboxRecord
            {
                AggregateType = "user",
                AggregateId = newUser.Id,
                Type = "userCreated",
                Payload = JsonSerializer.Serialize(new User
                {
                    Id = newUser.Id,
                    Name = "Adam"
                }, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                })
            };
            await _outboxCollection.InsertOneAsync(session, outboxRecord);

            await session.CommitTransactionAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred: {e.Message}");
            await session.AbortTransactionAsync();
        }
    }
}