using Newtonsoft.Json;
using System.Collections.Generic;

namespace POC.Kafka.Models;

public class Stats
{
    [JsonProperty("name")]
    public string Name { get; set; }

    [JsonProperty("topics")]
    public IReadOnlyDictionary<string, Topic> Topics { get; set; }

}
