using POC.Kafka.Configuration;
using POC.Kafka.Services;

namespace POC.Kafka;

public class Startup
{
    public Startup(IConfiguration configuration, IHostEnvironment environment)
    {
        _configuration = configuration;
        _environment = environment;
    }

    private IHostEnvironment _environment { get; }
    private IConfiguration _configuration { get; }

    // This method gets called by the runtime. Use this method to add services to the container.
    public void ConfigureServices(IServiceCollection services)
    {
        services.AddOptions();

        services.Configure<KafkaConfiguration>(_configuration.GetSection(nameof(KafkaConfiguration)));

        if (_environment.IsProduction())
        {
            // This gives the kafka container time to start up via docker-compose
            Thread.Sleep(TimeSpan.FromSeconds(30));
        }

        services.AddHostedService<ProducerService>();
        services.AddHostedService<ConsumerService>();

        services.AddControllers();
    }

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    public void Configure(IApplicationBuilder app)
    {
        if (_environment.IsDevelopment())
        {
            app.UseDeveloperExceptionPage();
        }

        app.UseHttpsRedirection();

        app.UseRouting();

        app.UseAuthorization();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapControllers();
        });
    }
}