﻿using System;
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
                try
                {
                    Thread.Sleep(500);
                    var dr = await p.ProduceAsync("my-topic-1", new Message<Null, string> { Value = "test" });
                    //I want to publish message every 500 millisecond.
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }

            Console.ReadLine();
        }
    }
}
