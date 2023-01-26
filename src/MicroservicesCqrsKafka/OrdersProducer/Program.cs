// See https://aka.ms/new-console-template for more information

using System.Text.Json;
using Common;
using Confluent.Kafka;

namespace OrdersProducer;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = Constants.BootstrapServers
        }).Build();

        for (int i = 0; i < 500; i++)
        {
            var order = new Order
            {
                Id = $"order_{i}",
                Product = (Product)(i % 3),
                Quantity = 1
            };
            await producer.ProduceAsync("orders",
                new Message<string, string>
                {
                    Key = order.Id,
                    Value = JsonSerializer.Serialize(order)
                });

            await Task.Delay(1000);
        }
    }
}