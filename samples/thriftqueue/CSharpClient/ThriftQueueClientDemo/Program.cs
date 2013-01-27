using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using Leansoft.BigQueue.Thrift;

namespace ThriftQueueClientDemo
{
    /// <summary>
    /// A sample big queue client which communicates with big queue server through Thrift RPC.
    /// 
    /// </summary>
    class Demo
    {
        public static readonly String SERVER_IP = "localhost";
        public static readonly int SERVER_PORT = 9000;
        public static readonly int TIME_OUT = 30000;
        public static readonly String TOPIC = "log";

        private TTransport transport = null;
        private BigQueueService.Client client = null;

        private void initClient()
        {
            transport = new TFramedTransport(new TSocket(SERVER_IP, SERVER_PORT, TIME_OUT));
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new BigQueueService.Client(protocol);
            transport.Open();
        }

        public void run()
        {
            try
            {
                this.initClient();

                Console.Out.WriteLine("big queue size before enqueue : " + client.getSize(TOPIC));

                QueueRequest req = new QueueRequest();
                req.Data = Encoding.Default.GetBytes("hello world");
                client.enqueue(TOPIC, req);

                Console.Out.WriteLine("big queue size after enqueue : " + client.getSize(TOPIC));

                QueueResponse resp = client.peek(TOPIC);
                Console.Out.WriteLine("big queue size after peek : " + client.getSize(TOPIC));
                Console.Out.WriteLine("peeked message : " + Encoding.Default.GetString(resp.Data));

                resp = client.dequeue(TOPIC);
                Console.Out.WriteLine("big queue size after dequeue : " + client.getSize(TOPIC));
                Console.Out.WriteLine("dequeued message : " + Encoding.Default.GetString(resp.Data));
            }
            finally
            {
                this.closeClient();
            }
        }

        private void closeClient()
        {
            if (transport != null)
            {
                transport.Close();
            }
        }

        static void Main(string[] args)
        {
            Demo demo = new Demo();
            demo.run();

            Console.Out.WriteLine("Demo finished, press any key to stop");
            Console.ReadKey();
        }
    }
}
