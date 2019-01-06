using System;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    class Program
    {
        private static async Task onCron(UInt16 id, TcpSession session, byte[] buffer)
        {
            String buf = System.Text.Encoding.Default.GetString(buffer);
            await session.send(buf, id);
        }
        private static void disConnectCallback(TcpSession session)
        {
            System.Console.WriteLine("my close : {0}", session.ID);
        }

        private static void connectCallback(TcpSession session)
        {
            System.Console.WriteLine("connect close : {0}", session.ID);
        }

        static void Main(string[] args)
        {
            var service = new TcpService();
            service.registerMsg(1, onCron);
            service.startListen("127.0.0.1", 9999, connectCallback, disConnectCallback);
            
            while (true)
            {
                Thread.Sleep(1000);
            }
        }
    }
}
