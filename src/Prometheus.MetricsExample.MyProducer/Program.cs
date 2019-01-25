using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Prometheus.MetricsExample.MyProducer
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            using (var p = new Producer<Null, string>(config))
            {
                while (true)
                {
                    try
                    {
                        var dr = await p.ProduceAsync("my-topic-1", new Message<Null, string> { Value = "test" });
                        Thread.Sleep(new Random().Next(100));
                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    }
                    catch (KafkaException e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                }
            }

            Console.ReadLine();
        }
    }
}
