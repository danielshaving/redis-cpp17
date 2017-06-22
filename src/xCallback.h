#pragma once
#include "all.h"

class xBuffer;
class xTcpconnection;
class xConnector;
class xRedisAsyncContext;
class xRedisContext;
class xRedisReader;

typedef std::shared_ptr<xRedisReader> xRedisReaderPtr;
typedef std::shared_ptr<xRedisContext> xRedisContextPtr;
typedef std::shared_ptr<xRedisAsyncContext> xRedisAsyncContextPtr;

typedef std::shared_ptr<xTcpconnection> xTcpconnectionPtr;
typedef std::shared_ptr<xConnector> xConnectorPtr;
typedef std::function<void()> xTimerCallback;
typedef std::function<int(void *,void *)> xCmpCallback;

typedef std::function<void (const xTcpconnectionPtr&,void *)> ConnectionCallback;
typedef std::function<void (void *)> ConnectionErrorCallback;
typedef std::function<void (const xTcpconnectionPtr&)> CloseCallback;
typedef std::function<void (const xTcpconnectionPtr&)> WriteCompleteCallback;
typedef std::function<void (const xTcpconnectionPtr&, size_t)> HighWaterMarkCallback;
typedef std::function<void (const xTcpconnectionPtr&,xBuffer*,void *)> MessageCallback;








