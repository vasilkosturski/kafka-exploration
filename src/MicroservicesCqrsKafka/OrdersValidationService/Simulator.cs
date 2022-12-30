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

        for (var i = 0; i < 10; i++)
        {
            var order = fixture
                .Build<Order>()
                .With(x => x.State, OrderState.Created)
                .Create();
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