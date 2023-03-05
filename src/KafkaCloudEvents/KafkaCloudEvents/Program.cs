using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.SystemTextJson;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaCloudEvents;

public class Program
{
    private const string TopicName = "users";
    private const string BootstrapServers = "localhost:9092,localhost:9093";

    public static async Task Main()
    {
        await CreateKafkaTopic();
        
        var cloudEvent = new CloudEvent
        {
            Id = "event-id",
            Type = "event-type",
            Source = new Uri("https://cloudevents.io/"),
            Time = DateTimeOffset.UtcNow,
            DataContentType = "application/cloudevents+json",
            Data = new User
            {
                Id = "UserId",
                FirstName = "John",
                LastName = "Doe"
            }
        };
        CloudEventFormatter formatter = new JsonEventFormatter<User>(SerializationOptions, new JsonDocumentOptions());
        var kafkaMessage = cloudEvent.ToKafkaMessage(ContentMode.Structured, formatter);

        var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };
        using var producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();
        await producer.ProduceAsync(TopicName, kafkaMessage);

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "cgid",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(new[] { TopicName });
        var consumedMessage = consumer.Consume().Message;
        var cloudEventMessage = consumedMessage.ToCloudEvent(formatter);
        var dataJsonElement = (User)cloudEventMessage.Data;
        
        Console.WriteLine(JsonSerializer.Serialize(dataJsonElement, SerializationOptions));
    }
    
    private static JsonSerializerOptions SerializationOptions => new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
    
    private static async Task CreateKafkaTopic()
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = BootstrapServers
        };

        var builder = new AdminClientBuilder(config);
        var client = builder.Build();
        try
        {
            await client.CreateTopicsAsync(new List<TopicSpecification>
            {
                new()
                {
                    Name = TopicName, 
                    ReplicationFactor = 1, 
                    NumPartitions = 2
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