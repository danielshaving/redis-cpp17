using System;
using System.Collections.Generic;
using System.Text;
using System.Net.Sockets;
using System.Net;
using System.Threading.Tasks;
using System.Threading;

namespace Server
{
    class TcpService
    {
        public delegate Task MsgCallback(UInt16 id, TcpSession client, byte[] buffer);
        private Dictionary<UInt16, MsgCallback> handlers = new Dictionary<UInt16, MsgCallback>();
        private Dictionary<long, TcpSession> sessions = new Dictionary<long, TcpSession>();
        private ReaderWriterLockSlim sessionLocks = new ReaderWriterLockSlim();
        private int nextId = 0;
        private List<TcpListener> listeners = new List<TcpListener>();
        private List<Task> listenerTasks = new List<Task>();
        private ReaderWriterLockSlim listenerLocks = new ReaderWriterLockSlim();
        public void registerMsg(UInt16 id, MsgCallback callback)
        {
            handlers.Add(id, callback);
        }
        public TcpSession findSession(long id)
        {
            TcpSession ret = null;
            sessionLocks.EnterReadLock();
            sessions.TryGetValue(id, out ret);
            sessionLocks.ExitReadLock();
            return ret;
        }

        public void removeSession(TcpSession session)
        {
            bool success = false;
            var disConnnectCallback = session.disCallback;

            sessionLocks.EnterWriteLock();
            success = sessions.Remove(session.ID);
            sessionLocks.ExitWriteLock();

            if (success && disConnnectCallback != null)
            {
                disConnnectCallback(session);
            }
        }

        public void waitCloseAll()
        {
            sessionLocks.EnterWriteLock();

            foreach (var listener in listeners)
            {
                listener.Stop();
            }

            foreach (var listenerTask in listenerTasks)
            {
                try
                {
                    listenerTask.Wait();
                }
                catch (AggregateException)
                {

                }
            }

            foreach (var session in sessions)
            {
                session.Value.close();
                session.Value.wait();
            }

            sessions.Clear();
            sessionLocks.ExitWriteLock();
        }
        public async Task processPacket(TcpSession session, byte[] buffer, UInt16 id)
        {
            MsgCallback callback;
            if (handlers.TryGetValue(id, out callback))
            {
                await callback(id, session, buffer);
            }
        }
        public void startListen(string ip, int port, Action<TcpSession> enterCallback, Action<TcpSession> disconnectCallback)
        {
            Console.WriteLine("listen {0}:{1}", ip, port);
            var server = new TcpListener(IPAddress.Parse(ip), port);
            server.Start();

            listenerLocks.EnterWriteLock();
            listeners.Add(server);
            listenerTasks.Add(Task.Run(async () =>
            {
                while (true)
                {
                    var client = await server.AcceptTcpClientAsync();
                    if (client != null)
                    {
                        newSessionTask(client, enterCallback, disconnectCallback);
                    }
                    else
                    {
                        break;
                    }
                }
            }));
            listenerLocks.ExitWriteLock();
        }
        private void newSessionTask(TcpClient client, Action<TcpSession> enterCallback, Action<TcpSession> disconnectCallback)
        {
            TcpSession session = null;
            sessionLocks.EnterWriteLock();
            nextId++;
            long id = (long)nextId << 32;
            id |= (long)dateTimeToUnixTimestamp(DateTime.Now);
            session = new TcpSession(this, client, id);
            sessions[id] = session;
            sessionLocks.ExitWriteLock();

            session.disCallback = disconnectCallback;
            session.run();

            if (enterCallback != null)
            {
                enterCallback(session);
            }
        }
        public static int dateTimeToUnixTimestamp(DateTime dateTime)
        {
            var start = new DateTime(1970, 1, 1, 0, 0, 0, dateTime.Kind);
            return Convert.ToInt32((dateTime - start).TotalSeconds);
        }
    }
}
