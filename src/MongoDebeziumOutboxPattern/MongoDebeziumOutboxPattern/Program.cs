using Confluent.Kafka;
using MongoDB.Bson.Serialization.Conventions;
using MongoDebeziumOutboxPattern;

ConfigMongo();
await new UserService().CreateUserAsync();
ConsumeMessage();

void ConfigMongo()
{
    var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
    ConventionRegistry.Register("camelCase", conventionPack, _ => true);
}

static void ConsumeMessage()
{
    var consumerConfig = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "cgid",
        AutoOffsetReset = AutoOffsetReset.Earliest
    };
    using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    consumer.Subscribe(new[] { "outbox.event.user" });

    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(10000));
    var consumedMessage = consumeResult.Message;
    var partition = consumeResult.Partition;
    var key = consumeResult.Message.Key;
    var offset = consumeResult.Offset;
    //var dataJson = JsonSerializer.Serialize(data, SerializationOptions);

    Console.WriteLine($"Partition: {partition} Key: {key} Offset: {offset} Data: {consumedMessage}");
}