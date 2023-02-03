﻿using System.Text.Json;
using Common;
using Confluent.Kafka;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;
using static Common.Utils;

namespace OrdersAggregator;

public static class Program
{
    private static Random rng = new();
    
    public static async Task Main(string[] args)
    {
        //RockDBReader.Read();

        var builder = new StreamBuilder();
        builder.Stream<string, string>("orders")
            .Peek((_, order) =>
                Console.WriteLine($"Consumed Order: {order}"))
            .GroupBy<int, Int32SerDes>((k, v) =>
            {
                var order = JsonSerializer.Deserialize<Order>(v);
                return (int)order.Product;
            })
            .Aggregate(
                () => 0,
                (_, _, acc) => acc + 1,
                RocksDb.As<int, int>("orders-products-quantities")
                    .WithKeySerdes<Int32SerDes>()
                    .WithValueSerdes<Int32SerDes>()
            )
            .ToStream()
            .Peek((product, quantity) => 
                Console.WriteLine($"Product: {(Product)product}, Quantity: {quantity}"));
        
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "test-app",
            BootstrapServers = Constants.BootstrapServers,
            AutoOffsetReset = AutoOffsetReset.Earliest,
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