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

            await _usersCollection.InsertOneAsync(_session, newUser);

            var outboxRecord = new OutboxRecord
            {
                AggregateType = "user",
                //AggregateId = ObjectId.GenerateNewId(),
                AggregateId = "some_id",
                Type = "userCreated",
                Payload = JsonSerializer.Serialize(new User
                {
                    Name = "Adam"
                }, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }) // Here we're using the whole User object as the payload.
            };
            await _outboxCollection.InsertOneAsync(_session, outboxRecord);
        
            await _session.CommitTransactionAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine("An error occurred: " + e.Message);
            await _session.AbortTransactionAsync();
        }
    }
}