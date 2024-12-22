using Ling.RabbitMQ;
using Ling.RabbitMQ.Producers;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRabbitMQ(options =>
{
    options.HostName = "localhost";
    options.Port = 5672;
    options.UserName = "guest";
    options.Password = "guest";
}).AddWorkQueueProducer();

builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapGet("/push/{msg}", async (IWorkQueueProducer queue, string msg) =>
{
    await queue.InvokeAsync("test_queue", new { Content = msg });
    return "ok";
})
.WithName("Enqueue");

await app.RunAsync();
