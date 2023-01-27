// See https://aka.ms/new-console-template for more information

using System.Text.Json;
using Common;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace OrdersProducer;

public static class Program
{
    public static async Task Main(string[] args)
    {
        await CreateKafkaTopic("orders", Constants.BootstrapServers);
        
        var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = Constants.BootstrapServers
        }).Build();

        for (int i = 0; i < 500; i++)
        {
            var order = new Order
            {
                Id = $"order_{i}",
                Product = (Product)(i % 2),
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
    
    private static async Task CreateKafkaTopic(string topicName, string bootstrapServers)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = bootstrapServers
        };

        var builder = new AdminClientBuilder(config);
        var client = builder.Build();
        try
        {
            await client.CreateTopicsAsync(new List<TopicSpecification>
            {
                new()
                {
                    Name = topicName, 
                    ReplicationFactor = 1, 
                    NumPartitions = 4
                }
            });
        }
        catch (CreateTopicsException e)
        {
            // do nothing in case of topic already exist
        }
        finally
        {
            client.Dispose();
        }
    }
}