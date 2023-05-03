using System;
using System.Threading;
using System.Threading.Tasks;
using Com.Vkontech.Kafkaproto;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaProtobuf;

class Program
{
    private const string Topic = "users";
    private const string BootstrapServers = "localhost:9092";
    private const string SchemaRegistryUrl = "http://localhost:8081";

    static async Task Main(string[] args)
    {   
        var cts = new CancellationTokenSource();
        
        var producer = Task.Run(() => StartProducer(cts.Token));
        var consumer = Task.Run(() => StartConsumer(cts.Token));
        
        Console.ReadKey();
        cts.Cancel();
        await Task.WhenAll(producer, consumer);
    }
    
    private static async Task StartProducer(CancellationToken ct)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };
        var schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = SchemaRegistryUrl
        };

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer = new ProducerBuilder<string, User>(producerConfig)
            .SetValueSerializer(new ProtobufSerializer<User>(schemaRegistry))
            .Build();
        
        Console.WriteLine($"{producer.Name} producing on {Topic}. Enter user names, q to exit.");

        var age = 15;
        string input;
        while ((input = Console.ReadLine()) != "q")
        {
            var user = new User { Name = input, Age = age++ };
            await producer.ProduceAsync(Topic, new Message<string, User> { Key = input, Value = user }, ct);
        }
    }
    
    private static void StartConsumer(CancellationToken ct)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "protobuf-example-consumer-group"
        };

        using var consumer = new ConsumerBuilder<string, User>(consumerConfig)
            .SetValueDeserializer(new ProtobufDeserializer<User>().AsSyncOverAsync())
            .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
            .Build();
        
        consumer.Subscribe(Topic);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (consumeResult is null) continue;
                    
                    var user = consumeResult.Message.Value;
                    Console.WriteLine($"key: {consumeResult.Message.Key}, user name: {user.Name}, age: {user.Age}");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            consumer.Close();
        }
    }
}