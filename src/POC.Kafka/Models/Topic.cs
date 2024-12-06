using Newtonsoft.Json;

namespace POC.Kafka.Models;

public class Topic
{
    [JsonProperty("topic_id")]
    public string TopicId { get; set; }

    [JsonProperty("partitions")]
    public IReadOnlyDictionary<string, Partition> Partitions { get; set; }
}
