using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using POC.Kafka.Configuration;
using POC.Kafka.Models;

namespace POC.Kafka.Services;

public class ConsumerService : IHostedService, IDisposable
{
    private readonly ILogger<ConsumerService> _logger;
    private readonly KafkaConfiguration _kafkaConfiguration;
    private IConsumer<string, string> _consumer;

    public ConsumerService(ILogger<ConsumerService> logger, IOptions<KafkaConfiguration> kafkaConfigurationOptions)
    {
        _logger = logger ?? throw new ArgumentException(nameof(logger));
        _kafkaConfiguration = kafkaConfigurationOptions?.Value ?? throw new ArgumentException(nameof(kafkaConfigurationOptions));

        Init();
    }
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("Kafka Consumer Service has started.");

                _consumer.Subscribe(new List<string>() { _kafkaConfiguration.Topic });

                await Consume(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Kafka Consumer Service is stopping.");

        _consumer.Close();

        await Task.CompletedTask;
    }

    public void Dispose()
    {
        _consumer.Dispose();
    }

    private void Init()
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = _kafkaConfiguration.Brokers,
            GroupId = _kafkaConfiguration.ConsumerGroup,
            SecurityProtocol = SecurityProtocol.Plaintext,
            EnableAutoCommit = false,
            StatisticsIntervalMs = 5000,
            SessionTimeoutMs = 6000,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true
        };

        _consumer = new ConsumerBuilder<string, string>(config).SetStatisticsHandler((_, kafkaStatistics) => LogKafkaStats(kafkaStatistics)).
            SetErrorHandler((_, e) => LogKafkaError(e)).Build();
    }

    private void LogKafkaStats(string kafkaStatistics)
    {
        var stats = JsonConvert.DeserializeObject<Stats>(kafkaStatistics);

        if (stats?.Topics != null && stats.Topics.Count > 0)
        {
            foreach (var topic in stats.Topics)
            {
                foreach (var partition in topic.Value.Partitions)
                {
                    Task.Run(() =>
                    {
                        var logMessage = $"KafkaStats Topic: {topic.Key} Partition: {partition.Key} PartitionConsumerLag: {partition.Value.ConsumerLag}";
                        _logger.LogInformation(logMessage);
                    });
                }
            }
        }
    }

    private void LogKafkaError(Error ex)
    {
        Task.Run(() =>
        {
            var error = $"Kafka Exception: ErrorCode:[{ex.Code}] Reason:[{ex.Reason}] Message:[{ex.ToString()}]";
            _logger.LogError(error);
        });
    }

    private async Task Consume(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(cancellationToken);

                if (consumeResult?.Message == null) continue;

                if (consumeResult.Topic.Equals(_kafkaConfiguration.Topic))
                {
                    await Task.Run(() =>
                    {
                        _logger.LogInformation($"[{consumeResult.Message.Key}] {consumeResult.Topic} - {consumeResult.Message.Value}");
                    }, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }
    }
}