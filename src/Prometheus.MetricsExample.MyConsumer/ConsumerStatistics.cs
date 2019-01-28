using System.Collections.Generic;
using Newtonsoft.Json;

namespace Prometheus.MetricsExample.MyConsumer
{
    public class ConsumerStatistics
    {
        [JsonProperty(PropertyName="topics")]
        public Dictionary<string, TopicStatistic> Topics { get; set; }

        public ConsumerStatistics()
        {
            Topics = new Dictionary<string, TopicStatistic>();
        }
    }

    public class TopicStatistic
    {
        [JsonProperty(PropertyName="partitions")]
        public Dictionary<string, PartitionStatistic> Partitions { get; set; }

        public TopicStatistic()
        {
            Partitions = new Dictionary<string, PartitionStatistic>();
        }
    }

    public class PartitionStatistic
    {
        [JsonProperty(PropertyName="consumer_lag")]
        public long ConsumerLag { get; set; }
    }
}