using System.Text.Json;
using AutoFixture;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace Simulator;

public class Program
{
    public static async Task Main(string[] args)
    {
        var fixture = new Fixture();
        
        var bootstrapServers = "localhost:9092";

        /*
        await CreateKafkaTopic("orders", bootstrapServers);
        await CreateKafkaTopic("warehouse.inventory", bootstrapServers);
        */

        var ordersProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();
        
        var inventoryProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();

        while (true)
        {
            var order = fixture.Create<Order>();
            var serializedOrder = JsonSerializer.Serialize(order);
            await ordersProducer.ProduceAsync("orders", new Message<string, string>
            {
                Key = order.Id,
                Value = serializedOrder
            });
            
            var inventory = new WarehouseInventory
            {
                Product = order.Product,
                Quantity = order.Quantity
            };
            var serializedInventory = JsonSerializer.Serialize(inventory);
            await inventoryProducer.ProduceAsync("warehouse.inventory", new Message<string, string>
            {
                Key = ((int)inventory.Product).ToString(),
                Value = serializedInventory
            });

            await Task.Delay(1000);
        }
    }
    
    private static async Task CreateKafkaTopic(string topicName, string bootstrapServers)
    {
        using var adminClient =
            new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        
        await adminClient.CreateTopicsAsync(new TopicSpecification[]
        {
            new() { Name = topicName, ReplicationFactor = 1, NumPartitions = 1 }
        });
    }
}

public class Order
{
    public string Id { get; set; }
    public OrderState State { get; set; }
    public Product Product { get; set; }
    public int Quantity { get; set; }
    public double Price { get; set; }
}

public class WarehouseInventory
{
    public Product Product { get; set; }
    public int Quantity { get; set; }
}

public enum OrderState
{
    Created,
    Validated,
    Failed,
    Shipped
}

public enum Product
{
    Jumpers, 
    Underpants, 
    Stockings
}