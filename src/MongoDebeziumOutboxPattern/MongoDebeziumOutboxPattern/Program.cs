using Confluent.Kafka;
using MongoDB.Bson.Serialization.Conventions;
using MongoDebeziumOutboxPattern;

ConfigMongo();

var cts = new CancellationTokenSource();

var producer = Task.Run(() => StartProducer(cts.Token));
var consumer = Task.Run(() => StartConsumer(cts.Token));

Console.ReadKey();
cts.Cancel();
await Task.WhenAll(producer, consumer);

static async Task StartProducer(CancellationToken ct)
{
    var userService = new UserService();
    while (!ct.IsCancellationRequested)
    {
        await userService.CreateRandomUser();
        await Task.Delay(TimeSpan.FromSeconds(1));
    }
}

static void StartConsumer(CancellationToken ct)
{
    var consumerConfig = new ConsumerConfig
    {
        BootstrapServers = "localhost:9092",
        GroupId = "cgid",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        AllowAutoCreateTopics = true
    };
    using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    consumer.Subscribe(new[] { "outbox.event.user" });

    while (!ct.IsCancellationRequested)
    {
        try
        {
            var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(500));

            if (consumeResult != null)
            {
                var consumedMessage = consumeResult.Message;
                var partition = consumeResult.Partition;
                var offset = consumeResult.Offset;

                Console.WriteLine($"Consumed Message. Partition: {partition} Offset: {offset} Data: {consumedMessage.Value}");
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Consume error: {e.Error.Reason}");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Unexpected error: {e}");
        }
    }
}

void ConfigMongo()
{
    var conventionPack = new ConventionPack { new CamelCaseElementNameConvention() };
    ConventionRegistry.Register("camelCase", conventionPack, _ => true);
}