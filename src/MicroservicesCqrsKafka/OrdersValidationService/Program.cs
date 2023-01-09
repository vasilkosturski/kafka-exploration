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
        await CreateKafkaTopic("orders", Simulator.BootstrapServers);
        await CreateKafkaTopic("warehouse.inventory", Simulator.BootstrapServers);

        var builder = new StreamBuilder();
        
        var inventoryTable = builder.Table("warehouse.inventory", 
            InMemory.As<string, string>());

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
            //.To("output-topic");
            .Foreach((k, v) => Console.WriteLine($"k: {k}, v: {v}"));
        
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = $"test-app-2",
            BootstrapServers = Simulator.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        var ordersStream = new KafkaStream(builder.Build(), config);

        Console.CancelKeyPress += (o, e) => {
            ordersStream.Dispose();
        };
        
        await ordersStream.StartAsync();

        _ = Task.Run(async () =>
        {
            while (true)
            {
                await Simulator.ProduceInventory();
                await Task.Delay(1500);
            }
        });
        
        _ = Task.Run(async () =>
        {
            while (true)
            {
                await Simulator.ProduceOrder();
                await Task.Delay(1000);
            }
        });
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