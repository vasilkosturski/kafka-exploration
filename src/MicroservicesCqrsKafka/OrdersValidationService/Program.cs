using System.Text.Json;
using Common;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace OrdersValidationService;

public static class Program
{
    private static Random rng = new Random();
    
    public static async Task Main(string[] args)
    {
        //RockDBReader.Read();
        
        await CreateKafkaTopic("orders", Constants.BootstrapServers);

        var builder = new StreamBuilder();

        builder.Stream<string, string>("orders")
            .SelectKey((k, v) =>
            {
                var order = JsonSerializer.Deserialize<Order>(v);
                return (int)order.Product;
            })
            .GroupByKey<Int32SerDes, StringSerDes>()
            .Aggregate(
                () => 0,
                (_, v, acc) =>
                {
                    var order = JsonSerializer.Deserialize<Order>(v);
                    return acc + order.Quantity;
                }, RocksDb.As<int, int>($"orders-products-quantities")
                    .WithKeySerdes<Int32SerDes>()
                    .WithValueSerdes<Int32SerDes>()
            )
            .ToStream()
            .Foreach((product, quantity) => 
                Console.WriteLine($"Product: {(Product)product}, Quantity: {quantity}"));
        
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-app",
            BootstrapServers = Constants.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            StateDir = $"./state/state-dir-{rng.Next()}",
            
            CommitIntervalMs = (long)TimeSpan.FromHours(1).TotalMilliseconds // Set for demo purposes
        };
        var ordersStream = new KafkaStream(builder.Build(), config);

        Console.CancelKeyPress += (o, e) => {
            ordersStream.Dispose();
        };
        
        await ordersStream.StartAsync();
    }
    
    private static async Task CreateKafkaTopic(string topicName, string bootstrapServers)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        };

        var builder = new AdminClientBuilder(config);
        var client = builder.Build();
        try
        {
            await client.CreateTopicsAsync(new List<TopicSpecification>
            {
                new()
                {
                    Name = topicName, 
                    ReplicationFactor = 1, 
                    NumPartitions = 2
                }
            });
        }
        catch (CreateTopicsException e)
        {
            // do nothing in case of topic already exist
        }
        finally
        {
            client.Dispose();
        }
    }
}