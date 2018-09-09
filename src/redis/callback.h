#pragma once
#include "all.h"
class Buffer;
class RedisAsyncContext;
class TcpConnection;
class Connector;
class RedisContext;
class RedisReader;
class HiredisAsync;
class TcpClient;
class Session;
class Item;
class ThreadPool;
class Acceptor;
class Channel;
class TimerQueue;
class Poll;
class Epoll;
class Thread;
class RedisObject;
class RedisReply;
class Timer;
class Select;
struct RedLockCallback;
class RedisAsyncCallback;

typedef std::shared_ptr<Timer> TimerPtr;
typedef std::weak_ptr<RedisReply> RedisReplyWeakPtr;
typedef std::shared_ptr<RedisReply> RedisReplyPtr;
typedef std::shared_ptr<RedisObject> RedisObjectPtr;
typedef std::shared_ptr<HiredisAsync> HiredisAsyncPtr;
typedef std::shared_ptr<Buffer> BufferPtr;
typedef std::shared_ptr<RedisReader> RedisReaderPtr;
typedef std::shared_ptr<RedisContext> RedisContextPtr;
typedef std::shared_ptr<RedisAsyncContext> RedisAsyncContextPtr;
typedef std::shared_ptr<RedisAsyncCallback> RedisAsyncCallbackPtr;
typedef std::list<RedisAsyncCallbackPtr> RedisAsyncCallbackList;
typedef std::shared_ptr<RedLockCallback> RedLockCallbackPtr;
typedef std::function<void(const RedisAsyncContextPtr &,
		const RedisReplyPtr &, const std::any &)> RedisCallbackFn;

typedef std::shared_ptr<TcpConnection> TcpConnectionPtr;
typedef std::shared_ptr<Connector> ConnectorPtr;
typedef std::shared_ptr<TcpClient> TcpClientPtr;
typedef std::shared_ptr<Session> SessionPtr;
typedef std::shared_ptr<Item> ItemPtr;
typedef std::shared_ptr<const Item> ConstItemPtr;
typedef std::shared_ptr<ThreadPool> ThreadPoolPtr;
typedef std::unique_ptr<Acceptor> AcceptorPtr;
typedef std::shared_ptr<Channel> ChannelPtr;
typedef std::shared_ptr<TimerQueue> TimerQueuePtr;
typedef std::shared_ptr<Poll> PollPtr;
typedef std::shared_ptr<Epoll> EpollPtr;
typedef std::shared_ptr<Select> SelectPtr;
typedef std::shared_ptr<Thread> ThreadPtr;
typedef std::function<void()> TimerCallback;
typedef std::function<void(const TcpConnectionPtr&)> ConnectionCallback;
typedef std::function<void(const TcpConnectionPtr&)> DisConnectionCallback;
typedef std::function<void(const std::any &)> ConnectionErrorCallback;
typedef std::function<void(const TcpConnectionPtr&)> CloseCallback;
typedef std::function<void(const TcpConnectionPtr&)> WriteCompleteCallback;
typedef std::function<void(const TcpConnectionPtr&, size_t)> HighWaterMarkCallback;
typedef std::function<void(const TcpConnectionPtr&, Buffer*)> MessageCallback;








