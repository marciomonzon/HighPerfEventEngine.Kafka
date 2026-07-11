using Consumer.Worker.Extensions;
using Consumer.Worker.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafka(builder.Configuration);

builder.Services.AddHostedService<OrderConsumerWorker>();

var host = builder.Build();

host.Run();