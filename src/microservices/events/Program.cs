using System.Text.Json;
using Confluent.Kafka;

var topics = new[] { "movie", "user", "payment" };
var brokers = Environment.GetEnvironmentVariable("KAFKA_BROKERS") ?? "kafka:9092";
var port = Environment.GetEnvironmentVariable("PORT") ?? "8082";

var builder = WebApplication.CreateSlimBuilder(args);
builder.Logging.AddSimpleConsole(o => o.SingleLine = true);


var producerConfig = new ProducerConfig
{
    BootstrapServers = brokers,
    Acks = Acks.Leader,
    MessageTimeoutMs = 10000,
    SocketTimeoutMs = 10000,
};

var producer = new ProducerBuilder<string, string>(producerConfig)
    .SetErrorHandler((_, e) => Console.WriteLine($"KAFKA PRODUCER [{e.Code}] {e.Reason}"))
    .SetLogHandler((_, m) => Console.WriteLine($"KAFKA [{m.Level}] {m.Message}"))
    .Build();

foreach (var topic in topics)
{
    var t = topic;
    Task.Run(() =>
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = brokers,
            GroupId = "events-service",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };
        while (true)
        {
            try
            {
                using var consumer = new ConsumerBuilder<string, string>(config).Build();
                consumer.Subscribe(t);
                Console.WriteLine($"CONSUMER запущен topic={t} group=events-service");
                while (true)
                {
                    var message = consumer.Consume();
                    Console.WriteLine(
                        $"CONSUMED topic={t} partition={message.Partition.Value} " +
                        $"offset={message.Offset.Value} key={message.Message.Key} " +
                        $"value={message.Message.Value}");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"CONSUMER error topic={t}: {e.Message}");
                Thread.Sleep(TimeSpan.FromSeconds(2)); // ждём Kafka и пробуем снова
            }
        }
    });
}

var app = builder.Build();

app.MapGet("/api/events/health", () => Results.Json(new { status = true }));

app.MapPost("/api/events/{type}", async (string type, HttpRequest request) =>
{
    if (!topics.Contains(type))
        return Results.BadRequest(new { error = $"unknown event type: {type}" });

    string payload;
    using (var reader = new StreamReader(request.Body))
        payload = await reader.ReadToEndAsync();

    var id = Guid.NewGuid().ToString();
    var @event = new
    {
        Id = id,
        Type = type,
        Timestamp = DateTime.UtcNow,
        Data = payload
    };

    try
    {
        var delivery = await producer.ProduceAsync(type, new Message<string, string>
        {
            Key = id,
            Value = JsonSerializer.Serialize(@event),
        });

        Console.WriteLine($"PRODUCED topic={type} id={id} partition={delivery.Partition.Value}");
        return Results.Created($"/api/events/{type}/{id}",
            new { status = "success", id, topic = type });
    }
    catch (ProduceException<string, string> e)
    {
        Console.WriteLine($"PRODUCE error topic={type}: {e.Error.Reason}");
        return Results.StatusCode(StatusCodes.Status500InternalServerError);
    }
});

await app.RunAsync();