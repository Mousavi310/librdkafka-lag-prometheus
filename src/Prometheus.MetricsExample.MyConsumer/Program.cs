using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Prometheus.MetricsExample.MyConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Task.Run(() => ConsumeMessages("my-consumer-group-1", "my-topic-1", 2000));
            Task.Run(() => ConsumeMessages("my-consumer-group-2", "my-topic-1", 5000));
            Console.ReadLine();
        }

        public static async Task ConsumeMessages(string groupId, string topicName, int delayInMilliseconds)
        {
            var conf = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };

            using (var c = new Consumer<Ignore, string>(conf))
            {
                c.Subscribe(topicName);

                bool consuming = true;
                c.OnError += (_, e) => consuming = !e.IsFatal;

                while (consuming)
                {
                    try
                    {
                        var cr = c.Consume();
                        Thread.Sleep(delayInMilliseconds);
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
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
