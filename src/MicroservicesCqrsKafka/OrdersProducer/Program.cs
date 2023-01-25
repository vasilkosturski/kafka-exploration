// See https://aka.ms/new-console-template for more information

using System.Text.Json;
using Common;
using Confluent.Kafka;

namespace OrdersProducer;

public static class Program
{
    private static readonly IProducer<string, string> OrdersProducer =
        new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = Constants.BootstrapServers
        }).Build();

    public static async Task Main(string[] args)
    {
        for (int i = 0; i < 500; i++)
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