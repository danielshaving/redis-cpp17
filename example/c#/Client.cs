using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace net
{
    class Program
    { 
        struct Message
        {
            public UInt16 cmd;
            public String data;
            public Socket client;
        }

        private delegate bool MessageCallback(Socket client, UInt16 cmd,String data);
        private static Dictionary<int, MessageCallback> dict = new Dictionary<int, MessageCallback>();

        private static int BUFFER = 65536;
        private static byte[] buffer = new byte[BUFFER];
        private static int writeIndex = 0;
        private static ConcurrentQueue<Message> messageQueue = new ConcurrentQueue<Message>(); 

        private static void ConnectCallback(Socket clientSocket)
        {
            Console.WriteLine("server connect ");
        }
        private static void DisconnectCallback(Socket clientSocket)
        {
            Console.WriteLine("server close");
            clientSocket.Shutdown(SocketShutdown.Both);
            clientSocket.Close();
        }
        private static void ReceiveCallback(Socket clientSocket,int bytesRead)
        {
            try
            {
                int readIndex = 0;
                writeIndex += bytesRead;
                while (bytesRead - readIndex >= sizeof(UInt16) * 3)
                {
                    UInt16 len = BitConverter.ToUInt16(buffer, readIndex);
                    if (bytesRead - readIndex - (sizeof(UInt16) * 3) < len)
                    {
                        Array.Copy(buffer, readIndex, buffer, 0,writeIndex);
                        break;
                    }

                    UInt16 flag = BitConverter.ToUInt16(buffer, readIndex + sizeof(UInt16));
                    UInt16 cmd = BitConverter.ToUInt16(buffer, readIndex + sizeof(UInt16) * 2);

                    string data = System.Text.Encoding.Default.GetString(buffer, readIndex + sizeof(UInt16) * 3, len);
                    Message msg;
                    msg.client = clientSocket;
                    msg.cmd = cmd;
                    msg.data = data;
                    messageQueue.Enqueue(msg);
   
                    writeIndex -= sizeof(UInt16) * 3 + len;
                    readIndex += sizeof(UInt16) * 3 + len;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        private static void EncodeTo(Socket client, UInt16 id, String data)
        {
            int writeIndex = 0;
            UInt16 len = (UInt16)(data.Length + sizeof(UInt16) * 3);
            UInt16 flag = 0;
            UInt16 cmd = id;

            byte[] data1 = BitConverter.GetBytes(data.Length);
            byte[] data2 = BitConverter.GetBytes(flag);
            byte[] data3 = BitConverter.GetBytes(cmd);
            byte[] data4 = System.Text.Encoding.UTF8.GetBytes(data);

            byte[] buffer = new byte[len];

            data1.CopyTo(buffer, writeIndex);
            data2.CopyTo(buffer, writeIndex + sizeof(UInt16));
            data3.CopyTo(buffer, writeIndex + sizeof(UInt16) * 2);
            data4.CopyTo(buffer, writeIndex + sizeof(UInt16) * 3);
            client.Send(buffer, buffer.Length, 0);
        }

        private static bool OnLogin(Socket client, UInt16 id, String data)
        {
            return true;
        }

        private static void RegisterMsg()
        {
            dict.Add(1, OnLogin);
        }

        public static void DoLogicThread()
        {
            while(true)
            {
                Message msg;
                while (messageQueue.TryDequeue(out msg))
                {
                    MessageCallback callback;
                    dict.TryGetValue(msg.cmd,out callback);
                    callback(msg.client,msg.cmd,msg.data);
                }
                Thread.Sleep(10);
            }
        }

        static void Main(string[] args)
        {  
            IPAddress ip = IPAddress.Parse("10.128.2.117");
            Socket clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                clientSocket.Connect(new IPEndPoint(ip, 8010));
                ConnectCallback(clientSocket);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                return;
            }

            RegisterMsg();

            Thread t1 = new Thread(new ThreadStart(DoLogicThread));
            UInt16 cmd = 2;
            EncodeTo(clientSocket, cmd, "test");

            while (true)
            {
                try
                {
                    int len = clientSocket.Receive(buffer, writeIndex, BUFFER - writeIndex, 0);
                    if (len <=0)
                    {
                        DisconnectCallback(clientSocket);
                        break;
                    }
                    ReceiveCallback(clientSocket, len);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    DisconnectCallback(clientSocket);
                    break;
                }
            }
        }
    }
}

