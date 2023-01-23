using System.Text.Json;
using Confluent.Kafka;

namespace OrdersValidationService;

public class Simulator
{
    public const string BootstrapServers = "localhost:9092,localhost:9093";

    private static readonly IProducer<string, string> OrdersProducer =
        new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers
        }).Build();

    public static async Task ProduceOrders()
    {
        for (int i = 0; i < 5; i++)
        {
            var order = new Order
            {
                Id = $"order_{i}",
                Product = (Product)(i % 2),
                Quantity = 1
            };
            await OrdersProducer.ProduceAsync("orders",
                new Message<string, string>
                {
                    Key = order.Id,
                    Value = JsonSerializer.Serialize(order)
                });

            await Task.Delay(1000);
        }
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