using System.Text.Json;
using Common;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;
using static Common.Utils;

namespace OrdersAggregator;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = new StreamBuilder();
        builder.Stream<string, string>("orders")
            .Peek((_, orderJson) =>
                Console.WriteLine($"Consumed Order: {orderJson}"))
            .GroupBy<int, Int32SerDes>((_, orderJson) =>
            {
                var order = JsonSerializer.Deserialize<Order>(orderJson);
                return (int)order.ProductType;
            })
            .Aggregate(
                () => 0,
                (_, _, acc) => acc + 1,
                RocksDb.As<int, int>(Constants.StateStoreName)
                    .WithKeySerdes<Int32SerDes>()
                    .WithValueSerdes<Int32SerDes>()
            )
            .ToStream()
            .Peek((product, quantity) => 
                Console.WriteLine($"Product Type: {(ProductType)product}, Quantity: {quantity}"));
        
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = Constants.ApplicationName,
            BootstrapServers = Constants.BootstrapServers,
            StateDir = GetStateDirectory(),
            
            CommitIntervalMs = (long)TimeSpan.FromHours(1).TotalMilliseconds // Set for demo purposes
        };
        var ordersStream = new KafkaStream(builder.Build(), config);

        Console.CancelKeyPress += (o, e) => {
            ordersStream.Dispose();
        };
        
        await ordersStream.StartAsync();
    }
}