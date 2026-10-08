using OutboxPublisher;
using Microsoft.EntityFrameworkCore;
using OutboxPublisher.Data;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<OutboxDbContext>(options =>
{
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("ProductDb"));
});
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
