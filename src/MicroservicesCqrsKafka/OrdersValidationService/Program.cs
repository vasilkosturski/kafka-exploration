using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace OrdersValidationService;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var bootstrapServers = "localhost:9092";
        
        await CreateKafkaTopic("orders", bootstrapServers);
        await CreateKafkaTopic("warehouse.inventory", bootstrapServers);
        
        _ = Task.Run(async () => await Simulator.Run(bootstrapServers));

        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = $"test-app-2",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            CommitIntervalMs = 3000
        };

        var builder = new StreamBuilder();
        var inventoryTable = builder.Table("warehouse.inventory", 
            InMemory.As<string, string>("table-store"));
        
        builder.Stream<string, string>("orders")
            .SelectKey((k, v) =>
            {
                var order = JsonSerializer.Deserialize<Order>(v);
                return ((int)order.Product).ToString();
            })
            .Filter((k, v) =>
            {
                var order = JsonSerializer.Deserialize<Order>(v);
                return order.State == OrderState.Created;
            })
            .Join(inventoryTable, (orderStr, inventoryStr) =>
            {
                var order = JsonSerializer.Deserialize<Order>(orderStr);
                var inventory = JsonSerializer.Deserialize<WarehouseInventory>(inventoryStr);
                return JsonSerializer.Serialize(new
                {
                    order, inventory
                });
            })
            .Foreach((k, v) => Console.WriteLine($"k: {k}, v: {v}"));
        
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
                    NumPartitions = 1
                }
            });
        }
        catch (Exception e)
        {
            // do nothing in case of topic already exist
        }
        finally
        {
            client.Dispose();
        }
    }
}