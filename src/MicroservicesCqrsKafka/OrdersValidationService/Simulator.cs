using System.Text.Json;
using AutoFixture;
using Confluent.Kafka;

namespace OrdersValidationService;

public class Simulator
{
    private static readonly Random Rng = new();
    
    public const string BootstrapServers = "localhost:9092,localhost:9093";

    private static readonly IProducer<string, string> OrdersProducer =
        new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers
        }).Build();

    public static async Task ProduceOrder()
    {
        var fixture = new Fixture();

        var productValues = Enum.GetValues(typeof(Product));
        var order = fixture
            .Build<Order>()
            .With(x => x.Product, (Product)productValues.GetValue(Rng.Next(productValues.Length)))
            .With(x => x.Quantity, Rng.Next(1,4))
            .Create();
        await OrdersProducer.ProduceAsync("orders",
            new Message<string, string>
            {
                Key = order.Id,
                Value = JsonSerializer.Serialize(order)
            });
    }
}

public class Order
{
    public string Id { get; set; }
    public Product Product { get; set; }
    public int Quantity { get; set; }
}

public enum Product
{
    Jackets,
    Shirts
}