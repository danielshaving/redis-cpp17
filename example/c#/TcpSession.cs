using System;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks.Dataflow;
using System.Text;
using System.Threading;

namespace Server
{
    class TcpSession
    {
        public class SendMsg
        {
            private byte[] msgBytes;
            public SendMsg(byte[] bytes)
            {
                msgBytes = bytes;
            }

            public byte[] msgData
            {
                get { return msgBytes; }
            }
        }

        private TcpClient client = null;
        private EndPoint endPoint = null;
        private BufferBlock<SendMsg> sendList = null;
        private TcpService service = null;
        private Action<TcpSession> disconnectCallback = null;
        private Task sendTask = null;
        private Task recvTask = null;
        private long Id = -1;
        private CancellationTokenSource calceRead = new CancellationTokenSource();
        public TcpSession(TcpService s, TcpClient c, long id)
        {
            client = c;
            service = s;
            Id = id;
            sendList = new BufferBlock<SendMsg>();
            endPoint = client.Client.RemoteEndPoint;
        }
		
		public long ID
		{
			set { Id = value; }
            get { return Id; }
		}
        public UInt16 reverseBytes(UInt16 value)
        {
            return (UInt16)((value & 0xFFU) << 8 | (value & 0xFF00U) >> 8);
        }
        public Action<TcpSession> disCallback
		{
			get { return disconnectCallback; }
			set { disconnectCallback = value; }
		}
		
		public void run()
		{
			if (sendTask == null)
			{
				sendThread();
			}
			
			if (recvTask == null)
			{
				recvThread();
			}
		}
		
		~TcpSession()
		{
			client?.Close();
			sendTask?.Dispose();
			recvTask?.Dispose();
		}
		
		public void wait()
		{
			if (recvTask != null)
			{
				recvTask.Wait();
			}
			
			if (sendTask != null)
			{
				sendTask.Wait();
			}
		}

		public void shutdown()
		{
			client.Client.Shutdown(SocketShutdown.Both);
		}
		
		public void close()
		{
			client.Close();
			calceRead.Cancel();
			sendList.Post(null);
		}

		public async Task Send(byte[] data)
		{
			var msg = new SendMsg(data);
			await sendList.SendAsync(msg);
		}

        private void sendThread()
        {
            sendTask = Task.Run(async () =>
            {
                try
                {
                    var writer = client.GetStream();
                    while (true)
                    {
                        var msg = await sendList.ReceiveAsync();
                        if (msg != null)
                        {
                            await writer.WriteAsync(msg.msgData, 0, msg.msgData.Length);
                            await writer.FlushAsync();
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                finally
                {
                    onClose();
                }
            });
        }

        private void recvThread()
        {
            recvTask = Task.Run(async () =>
            {
                try
                {
                    byte[] headBuffer = new byte[3 * sizeof(UInt16)];
                    var stream = client.GetStream();
                    var cancelToken = calceRead.Token;
                    while (true)
                    {
                        await stream.ReadAsync(headBuffer, 0, headBuffer.Length, cancelToken);
                        if (cancelToken.IsCancellationRequested)
                        {
                            break;
                        }

                        UInt16 len = reverseBytes(BitConverter.ToUInt16(headBuffer, 0));
                        UInt16 flag = reverseBytes(BitConverter.ToUInt16(headBuffer, sizeof(UInt16)));
                        UInt16 cmd = reverseBytes(BitConverter.ToUInt16(headBuffer, sizeof(UInt16) * 2));

                        byte[] body = new byte[len];
                        await stream.ReadAsync(body, 0, len, cancelToken);
                        if (cancelToken.IsCancellationRequested)
                        {
                            break;
                        }
    
                        await service.processPacket(this, body, cmd);
                        if (cancelToken.IsCancellationRequested)
                        {
                            break;
                        }
                    }
                }
                finally
                {
                    onClose();
                }
            });
        }
        private void onClose()
        {
            close();
            service.removeSession(this);
        }

        public async Task send(String data, UInt16 id)
		{
            int writeIndex = 0;
            UInt16 len = reverseBytes((UInt16)(data.Length + sizeof(UInt16) * 3));
            UInt16 flag = 0;
            UInt16 cmd = reverseBytes(id);

            byte[] data1 = BitConverter.GetBytes(data.Length);
            byte[] data2 = BitConverter.GetBytes(flag);
            byte[] data3 = BitConverter.GetBytes(cmd);
            byte[] data4 = System.Text.Encoding.UTF8.GetBytes(data);

            byte[] buffer = new byte[len];

            data1.CopyTo(buffer, writeIndex);
            data2.CopyTo(buffer, writeIndex + sizeof(UInt16));
            data3.CopyTo(buffer, writeIndex + sizeof(UInt16) * 2);
            data4.CopyTo(buffer, writeIndex + sizeof(UInt16) * 3);
            await Send(buffer);
		}
    }
}
