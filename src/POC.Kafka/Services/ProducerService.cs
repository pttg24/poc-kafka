using Confluent.Kafka;
using Microsoft.Extensions.Options;
using POC.Kafka.Configuration;
using POC.Kafka.Models;
using System.Text;

namespace POC.Kafka.Services
{
    public class ProducerService : IHostedService, IDisposable
    {
        private IProducer<string, string> _producer;
        private readonly ILogger<ProducerService> _logger;
        private readonly KafkaConfiguration _kafkaConfiguration;

        public ProducerService(ILogger<ProducerService> logger, IOptions<KafkaConfiguration> kafkaConfigurationOptions)
        {
            _logger = logger ?? throw new ArgumentException(nameof(logger));
            _kafkaConfiguration = kafkaConfigurationOptions?.Value ?? throw new ArgumentException(nameof(kafkaConfigurationOptions));

            Init();
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("Kafka Producer Service has started.");
                    await Produce(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Kafka Producer Service is stopping.");

            _producer.Flush(cancellationToken);

            await Task.CompletedTask;
        }

        public void Dispose()
        {
            _producer.Dispose();
        }

        private void Init()
        {
            var config = new ProducerConfig()
            {
                BootstrapServers = _kafkaConfiguration.Brokers,
                ClientId = "POC.Kafka",
                SecurityProtocol = SecurityProtocol.Plaintext,
                EnableDeliveryReports = false,
                QueueBufferingMaxMessages = 10000000,
                QueueBufferingMaxKbytes = 100000000,
                BatchNumMessages = 500,
                Acks = Acks.None,
                DeliveryReportFields = "none"
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        private async Task Produce(CancellationToken cancellationToken)
        {
            try
            {
                using (_logger.BeginScope("Kafka App Produce Sample Data"))
                {
                    if (!cancellationToken.IsCancellationRequested)
                    {
                        for (int i = 0; i < 1001; i++)
                        {
                            var json = new DataLoad().ToString();

                            var msg = new Message<string, string>
                            {
                                Key = _kafkaConfiguration.Key,
                                Value = json
                            };
                            await _producer.ProduceAsync(_kafkaConfiguration.Topic, msg, cancellationToken).ConfigureAwait(false);
                        }
                    }
                }
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, exception.Message);
            }
        }
    }
}