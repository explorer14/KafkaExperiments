using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

Host.CreateDefaultBuilder(args)
.ConfigureServices(services => services.AddHostedService<WarehouseEventConsumer>())
.Build()
.Run();

public record ProductStockChange(int productId, int purchaseOrderLineId, int quantityChange);

public class ProductStockChangeDeserialiser : IDeserializer<ProductStockChange>
{
    public ProductStockChange Deserialize(
        ReadOnlySpan<byte> data,
        bool isNull,
        SerializationContext context) =>
        JsonConvert.DeserializeObject<ProductStockChange>(Encoding.UTF8.GetString(data));
}

public class GuidKeyDeserialiser : IDeserializer<Guid>
{
    public Guid Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        Guid.Parse(Encoding.UTF8.GetString(data));
}

public class WarehouseEventConsumer : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = new ConsumerBuilder<Guid, ProductStockChange>(
            new[]
            {
                new KeyValuePair<string, string>("bootstrap.servers","localhost:29092"),
                new KeyValuePair<string, string>("group.id","1")
            })
            .SetValueDeserializer(new ProductStockChangeDeserialiser())
            .SetKeyDeserializer(new GuidKeyDeserialiser())
            .Build();
        consumer.Subscribe("warehouse-events");

        return Task.Factory.StartNew(() =>
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var msg = consumer.Consume();
                Console.WriteLine($"Message Id: {msg.Message.Key} " +
                    $"Payload: {msg.Message.Value} " +
                    $"read from partition {msg.TopicPartitionOffset.TopicPartition.Partition.Value} " +
                    $"and offset {msg.TopicPartitionOffset.Offset.Value}");

                consumer.Commit(msg);
            }
        });
    }
}