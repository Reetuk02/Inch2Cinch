using OrderService;
using Microsoft.EntityFrameworkCore;





var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddDbContext<OrderDbContext>(options =>
{
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("OrderDb"));
});
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
