using System.Text.Json;
using AutoFixture;
using Confluent.Kafka;

namespace OrdersValidationService;

public class Simulator
{
    public static async Task Run(string bootstrapServers)
    {
        var fixture = new Fixture();
        
        var ordersProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();
        
        var inventoryProducer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = bootstrapServers
        }).Build();

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
            await inventoryProducer.ProduceAsync("warehouse.inventory", new Message<string, string>
            {
                Key = ((int)inventory.Product).ToString(),
                Value = serializedInventory
            });
            
            var order = fixture
                .Build<Order>()
                .With(x => x.Product, inventory.Product)
                .With(x => x.State, OrderState.Created)
                .Create();
            var serializedOrder = JsonSerializer.Serialize(order);
            await ordersProducer.ProduceAsync("orders", new Message<string, string>
            {
                Key = order.Id,
                Value = serializedOrder
            });

            await Task.Delay(1000);
        }
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