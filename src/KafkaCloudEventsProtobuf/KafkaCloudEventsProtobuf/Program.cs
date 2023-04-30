﻿using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AutoFixture;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using CloudNative.CloudEvents.Kafka;
using CloudNative.CloudEvents.Protobuf;
using Confluent.Kafka;
using Google.Protobuf;

namespace KafkaCloudEventsProtobuf;

class Program
{
    private static readonly List<string> SamplePetNames = new()
    {
        "Bella",
        "Charlie",
        "Daisy",
        "Luna",
        "Max",
        "Milo",
        "Oliver",
        "Rocky",
        "Sadie",
        "Zeus"
    };
    
    private const string Topic = "animals";
    private const string BootstrapServers = "localhost:9092";

    private static readonly IFixture Fixture = new Fixture();

    static async Task Main(string[] args)
    {
        var cts = new CancellationTokenSource();
        var formatter = new ProtobufEventFormatter();
        var consumer = Task.Run(() => StartConsumer(formatter, cts.Token));
        await StartProducer(formatter, cts.Token);
        cts.Cancel();
        await consumer;
    }

    private static Animal ProduceAnimal()
    {
        var randomInt = Fixture.Create<int>();

        var animalBuilder = Fixture
            .Build<Animal>()
            .With(x => x.Name, SamplePetNames[randomInt % SamplePetNames.Count])
            .With(x => x.Age, randomInt % 10 + 1);
        
        var animalTypIndex = randomInt % 3;
        if (animalTypIndex == 0)
            return animalBuilder.With(x => x.Cat).Create();
        if (animalTypIndex == 1)
            return animalBuilder.With(x => x.Dog).Create();
        return animalBuilder.With(x => x.Bird).Create();
    }
    
    private static async Task StartProducer(ProtobufEventFormatter formatter, CancellationToken ct)
    {
        var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };
        using var producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();
        
        var i = 1;
        while (!ct.IsCancellationRequested)
        {
            var userId = $"UserId_{i}";
            var cloudEvent = new CloudEvent
            {
                Id = Guid.NewGuid().ToString(),
                Type = "event-type",
                Source = new Uri("https://cloudevents.io/"),
                Time = DateTimeOffset.UtcNow,
                DataContentType = "application/protobuf",
                Data = ProduceAnimal().ToByteArray()
            };
            cloudEvent.SetPartitionKey(userId);
            var kafkaMessage = cloudEvent.ToKafkaMessage(ContentMode.Binary, formatter);
            await producer.ProduceAsync(Topic, kafkaMessage, ct);
                
            i++;

            await Task.Delay(TimeSpan.FromSeconds(1), ct);
        }
    }
    
    private static void StartConsumer(ProtobufEventFormatter formatter, CancellationToken ct)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = "cgid",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = false
        };
        using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(new[] { Topic });

        while (!ct.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1000));
            if (consumeResult is null) continue;
                
            var consumedMessage = consumeResult.Message;
            var cloudEventMessage = consumedMessage.ToCloudEvent(formatter);
            var animal = Animal.Parser.ParseFrom((byte[])cloudEventMessage.Data);

            if (animal.AnimalTypeCase == Animal.AnimalTypeOneofCase.Cat)
                Console.WriteLine($"Name: {animal.Name}. Age: {animal.Age}. Color: {animal.Cat.Color}");
            if (animal.AnimalTypeCase == Animal.AnimalTypeOneofCase.Bird)
                Console.WriteLine($"Name: {animal.Name}. Age: {animal.Age}. CanFly: {animal.Bird.CanFly}");
            if (animal.AnimalTypeCase == Animal.AnimalTypeOneofCase.Dog)
                Console.WriteLine($"Name: {animal.Name}. Age: {animal.Age}. Breed: {animal.Dog.Breed}");    
        }
    }
}