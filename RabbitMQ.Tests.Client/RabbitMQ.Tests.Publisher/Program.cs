using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Tests.Publisher
{
    class Program
    {
        static void Main(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    //create an exchange named messages with type fanout. A fanout distributes all messages it receives to all queues it knows.
                    //DEFAULT: The exchange is declared non-passive, non-durable, non-autodelete, and non-internal
                    channel.ExchangeDeclare("messages", "fanout");

                    DateTime start = DateTime.Now;
                    int numOfMessages = 1200;


                    List<byte> body = new List<byte>();

                    for (int j = 0; j < 20000; j++)
                        body.Add(0x01);

                    byte[] bodyArray = body.ToArray();

                    Console.WriteLine("Writing {0} messages at {1} bytes each", numOfMessages, bodyArray.Length);

                    //create parallel execution
                    ParallelLoopResult result = Parallel.For(1, numOfMessages, (i) =>
                    {
                        if (i == numOfMessages - 1)
                            channel.BasicPublish("messages", String.Empty, null, Encoding.UTF8.GetBytes("last"));
                        else
                            channel.BasicPublish("messages", "", null, bodyArray);
                    });

                    while(!result.IsCompleted)
                    {

                    }

                    double totalSeconds = DateTime.Now.Subtract(start).TotalSeconds;
                    Console.WriteLine("Sent all messages after: {0} seconds", totalSeconds);
                    Console.WriteLine("{0} messages per second", numOfMessages / totalSeconds);
                    Console.ReadLine();
                }
            }
        }
    }
}
