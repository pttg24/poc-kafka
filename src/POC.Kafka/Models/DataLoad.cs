using Newtonsoft.Json;

namespace POC.Kafka.Models;

public class DataLoad
{
    public Guid Id { get; set; }
    public double Value { get; set; }
    public DateTime Created { get; set; }

    public DataLoad()
    {
        Id = Guid.NewGuid();
        Value = new Random().NextDouble();
        Created = DateTime.UtcNow;
    }

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this, new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
    }
}