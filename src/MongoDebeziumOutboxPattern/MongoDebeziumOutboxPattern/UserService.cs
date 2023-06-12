using System.Text.Json;
using MongoDB.Driver;

namespace MongoDebeziumOutboxPattern;

public class UserService
{
    private readonly IMongoCollection<User> _usersCollection;
    private readonly IMongoCollection<OutboxRecord> _outboxCollection;
    private readonly IClientSessionHandle _session;

    public UserService()
    {
        var client = new MongoClient("mongodb://localhost:27017");
        var database = client.GetDatabase("testdb");
        _usersCollection = database.GetCollection<User>("users");
        _outboxCollection = database.GetCollection<OutboxRecord>("outbox");
        _session = client.StartSession();
    }

    public async Task CreateUserAsync()
    {
        _session.StartTransaction();

        try
        {
            var newUser = new User
            {
                Name = "Adam"
            };
            await _usersCollection.InsertOneAsync(newUser);

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
            await _outboxCollection.InsertOneAsync(outboxRecord);

            await _session.CommitTransactionAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine("An error occurred: " + e.Message);
            await _session.AbortTransactionAsync();
        }
    }
}