namespace Prometheus.MetricsExample.MyConsumer
{
    public class ConsumerLag
    {
        public string Topic { get; set; }
        public string Partition { get; set; }
        public long Lag { get; set; }
    }
}