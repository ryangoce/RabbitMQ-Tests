using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Tests.Subscriber
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

                    //     DEFAULT
                    //     The queue is declared non-passive, non-durable, but exclusive and autodelete,
                    //     with no arguments. The server autogenerates a name for the queue - the generated
                    //     name is the return value of this method.
                    var queueName = channel.QueueDeclare().QueueName;

                    //bind the queue to the exchange
                    channel.QueueBind(queueName, "messages", String.Empty);

                    Console.WriteLine("Waiting for messages");

                    var consumer = new EventingBasicConsumer();

                    DateTime start = DateTime.Now;

                    int receivedCount = 0;
                    object receiveLock = new object();
                    int numOfMessages = 1200;

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);

                        lock(receiveLock)
                        {
                            if (receivedCount == 0)
                                start = DateTime.Now;

                            receivedCount++;
                            if (receivedCount == numOfMessages - 1)
                            {
                                double totalSeconds = DateTime.Now.Subtract(start).TotalSeconds;
                                Console.WriteLine("Received all messages after: {0} seconds", totalSeconds);
                            }
                        }

                        
                    };

                    channel.BasicConsume(queueName, true, consumer);

                    Console.ReadLine();
                }
            }
        }
    }
}
