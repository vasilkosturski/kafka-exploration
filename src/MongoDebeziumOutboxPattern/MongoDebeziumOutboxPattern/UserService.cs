using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoDebeziumOutboxPattern;

public class User
{
    public ObjectId Id { get; set; }
    public string Name { get; set; }
    // Add other fields as needed...
}

public class OutboxRecord
{
    public ObjectId Id { get; set; }
    public string AggregateType { get; set; }
    public ObjectId AggregateId { get; set; }
    public string Type { get; set; }
    public object Payload { get; set; }
}

public class UserService
{
    private readonly IMongoCollection<User> _usersCollection;
    private readonly IMongoCollection<OutboxRecord> _outboxCollection;
    private readonly IClientSessionHandle _session;

    public UserService(IMongoClient client)
    {
        var database = client.GetDatabase("testdb");
        _usersCollection = database.GetCollection<User>("users");
        _outboxCollection = database.GetCollection<OutboxRecord>("outbox");
        _session = client.StartSession();
        
        BsonClassMap.RegisterClassMap<User>(cm =>
        {
            cm.AutoMap();
            cm.SetIgnoreExtraElements(true);
        });
    }

    public async Task CreateUserAsync(string name)
    {
        _session.StartTransaction();

        try
        {
            var newUser = new User
            {
                Name = name,
                // Fill in additional fields...
            };

            await _usersCollection.InsertOneAsync(_session, newUser);

            var outboxRecord = new OutboxRecord
            {
                AggregateType = "user",
                AggregateId = newUser.Id,
                Type = "userCreated",
                Payload = newUser // Here we're using the whole User object as the payload.
            };

            await _outboxCollection.InsertOneAsync(_session, outboxRecord);

            await _session.CommitTransactionAsync();
            Console.WriteLine("User created and outbox record inserted successfully.");
        }
        catch (Exception e)
        {
            Console.WriteLine("An error occurred: " + e.Message);
            await _session.AbortTransactionAsync();
        }
    }
}