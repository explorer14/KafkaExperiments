using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

var producer = new ProducerBuilder<Guid, ProductStockChange>(
                new[]
                {
                    // TODO: should come from IConfiguration
                    new KeyValuePair<string, string>("bootstrap.servers","localhost:29092"),
                    new KeyValuePair<string, string>("enable.idempotence","true"),
                    new KeyValuePair<string, string>("acks","all")
                })
    .SetValueSerializer(new ProductStockChangeSerialiser())    
    .SetKeySerializer(new GuidKeySerialiser())
    .Build();

var rng = new Random();

while (true)
{
    var key = Console.ReadKey();

    if (key.Key == ConsoleKey.Enter)
    {
        // TODO: topic name to come from IConfiguration
        var delivery = await producer.ProduceAsync("warehouse-events", new Message<Guid, ProductStockChange>
        {
            Key = Guid.NewGuid(),
            Value = new ProductStockChange(
                rng.Next(1000, 99999),
                rng.Next(1000, 99999),
                rng.Next(-999, 999)),
            Timestamp = new Timestamp(DateTime.UtcNow)
        });

        Console.WriteLine(delivery.Status);
    }
}

// Events
public record ProductStockChange(int productId, int purchaseOrderLineId, int quantityChange);

public class ProductStockChangeSerialiser : ISerializer<ProductStockChange>
{
    public byte[] Serialize(ProductStockChange data, SerializationContext context) =>
        Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
}

public class GuidKeySerialiser : ISerializer<Guid>
{
    public byte[] Serialize(Guid data, SerializationContext context) =>
        Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());
}