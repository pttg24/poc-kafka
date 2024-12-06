using Newtonsoft.Json;

namespace POC.Kafka.Models;

public class Partition
{
    [JsonProperty("partition_id")]
    public int PartitionId { get; set; }

    [JsonProperty("consumer_lag")]
    public int ConsumerLag { get; set; }
}