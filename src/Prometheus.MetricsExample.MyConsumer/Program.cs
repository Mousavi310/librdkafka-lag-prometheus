using System;
using System.Collections;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;

namespace Prometheus.MetricsExample.MyConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Task.Run(() => ConsumeMessages("my-client-1", "my-consumer-group-1", "my-topic-1", 2000));
            Task.Run(() => ConsumeMessages("my-client-2", "my-consumer-group-2", "my-topic-1", 5000));
            
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
                    var consumerLag = ExtractConsumerLag(json);

                    if(consumerLag != null)
                    {
                        var gauge = Metrics.CreateGauge("librdkafka_consumer_lag", "store consumer lags", new GaugeConfiguration{
                            LabelNames = new []{"topic", "partition", "consumerGroup"}
                        });

                        gauge.WithLabels(consumerLag.Topic, consumerLag.Partition, groupId).Set(consumerLag.Lag);
                    }
                };

                while (consuming)
                {
                    try
                    {
                        var cr = c.Consume();
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}' in group {groupId}.");
                        Thread.Sleep(new Random().Next(delayInMilliseconds));
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
                c.Close();
            }
        }

        public static ConsumerLag ExtractConsumerLag(string json)
        {
            dynamic jsonObject = JObject.Parse(json);

            try
            {
                
                foreach (var topic in jsonObject.topics.Properties())
                {
                    foreach(var topicProperty in topic.Value.Properties())
                    {
                        if(topicProperty.Name != "partitions") continue;

                        foreach(var partition in topicProperty.Value.Properties())
                        {
                            foreach(var field in partition.Value.Properties())
                            {
                                if(field.Name == "consumer_lag")
                                {
                                    return new ConsumerLag
                                    {
                                        Lag = field.Value,
                                        Topic = topicProperty.Name,
                                        Partition = partition.Name
                                    };
                                }
                            }
                        }
                    }
                }
            }
            catch (System.Exception ex)
            {
                
            }

            return null;
        }
    }
}
