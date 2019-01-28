using System;
using System.Collections;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Prometheus.MetricsExample.MyConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Task.Run(() => ConsumeMessages("my-client-1", "my-consumer-group-1", "my-topic-1", 100));
            Task.Run(() => ConsumeMessages("my-client-2", "my-consumer-group-2", "my-topic-1", 110));
            
            var metricServer = new MetricServer(port: 7075);
            metricServer.Start();
            Console.ReadLine();
        }

        public static async Task ConsumeMessages(string clientId, string groupId, string topicName, int delayInMilliseconds)
        {
            var conf = new ConsumerConfig
            {
                GroupId = groupId,
                ClientId = clientId,
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetResetType.Earliest,
                StatisticsIntervalMs = 1000
            };

            using (var c = new Consumer<Ignore, string>(conf))
            {
                c.Subscribe(topicName);

                bool consuming = true;
                c.OnError += (_, e) => consuming = !e.IsFatal;
                
                c.OnStatistics += (_, json) => {
                    var statistics = JsonConvert.DeserializeObject<ConsumerStatistics>(json);

                    foreach(var topic in statistics.Topics)
                    {
                        foreach(var partition in topic.Value.Partitions)
                        {
                            var gauge = Metrics.CreateGauge("librdkafka_consumer_lag", "store consumer lags", new GaugeConfiguration{
                                LabelNames = new []{"topic", "partition", "consumerGroup"}
                            });

                            gauge.WithLabels(topic.Key, partition.Key, groupId).Set(partition.Value.ConsumerLag);
                        }
                    }
                };

                while (consuming)
                {
                    try
                    {
                        var cr = c.Consume();
                        Thread.Sleep(new Random().Next(delayInMilliseconds));
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}' in group {groupId}.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
                c.Close();
            }
        }
    }
}
