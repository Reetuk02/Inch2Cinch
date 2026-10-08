using Microsoft.EntityFrameworkCore;
using StockService;
using StockService.Data;
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<StockDbContext>(options =>
{
    options.UseSqlServer(
        builder.Configuration
            .GetConnectionString("StockDb"));
});
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();
