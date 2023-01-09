using System.Text.Json;
using AutoFixture;
using Confluent.Kafka;

namespace OrdersValidationService;

public class Simulator
{
    private static Random rng = new();
    
    public const string BootstrapServers = "localhost:9092,localhost:9093";
    
    private static readonly IProducer<string, string> InventoryProducer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = BootstrapServers
    }).Build();
    
    private static readonly IProducer<string, string> OrdersProducer = new ProducerBuilder<string, string>(new ProducerConfig
    {
        BootstrapServers = BootstrapServers
    }).Build();
    
    public static async Task ProduceInventory()
    {
        var inventories = new List<WarehouseInventory>
        {
            new()
            {
                Product = Product.Jumpers,
                Quantity = 10
            },
            new()
            {
                Product = Product.Stockings,
                Quantity = 10
            },
            new()
            {
                Product = Product.Underpants,
                Quantity = 10
            }
        };

        foreach (var inventory in inventories)
        {
            var serializedInventory = JsonSerializer.Serialize(inventory);
            //await InventoryProducer.ProduceAsync(new TopicPartition("warehouse.inventory", new Partition(GetPartition())),
            await InventoryProducer.ProduceAsync("warehouse.inventory",
                new Message<string, string>
                {
                    Key = ((int)inventory.Product).ToString(),
                    Value = serializedInventory
                });
        }
    }
    
    public static async Task ProduceOrder()
    {
        var fixture = new Fixture();

        var order = fixture
            .Build<Order>()
            .With(x => x.State, OrderState.Created)
            .Create();
        var serializedOrder = JsonSerializer.Serialize(order);
        //await OrdersProducer.ProduceAsync(new TopicPartition("orders", new Partition(GetPartition())),
        await OrdersProducer.ProduceAsync("orders",
            new Message<string, string>
            {
                Key = order.Id,
                Value = serializedOrder
            });
    }

    private static int GetPartition()
    {
        const int partitionsCount = 2;
        return rng.Next() % partitionsCount;
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