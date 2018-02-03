#pragma once
#include "all.h"


class xBuffer;
class xRedisAsyncContext;
class xTcpconnection;
class xConnector;
class xRedisContext;
class xRedisReader;
class xHiredisAsync;
class xTcpClient;
class xSession;

typedef std::shared_ptr<xRedisReader>  xRedisReaderPtr;
typedef std::shared_ptr<xRedisContext> xRedisContextPtr;
typedef std::shared_ptr<xHiredisAsync> xHiredisAsyncPtr;
typedef std::shared_ptr<xRedisAsyncContext> xRedisAsyncContextPtr;
typedef std::shared_ptr<xBuffer> xBufferPtr;
typedef std::shared_ptr<xTcpconnection> xTcpconnectionPtr;
typedef std::shared_ptr<xConnector> xConnectorPtr;
typedef std::shared_ptr<xTcpClient> xTcpClientPtr;
typedef std::shared_ptr<xSession>  xSeesionPtr;

typedef std::function<void (const std::any &)> xTimerCallback;
typedef std::function<void (const xTcpconnectionPtr&)> ConnectionCallback;
typedef std::function<void (const std::any &)> ConnectionErrorCallback;
typedef std::function<void (const xTcpconnectionPtr&)> CloseCallback;
typedef std::function<void (const xTcpconnectionPtr&)> WriteCompleteCallback;
typedef std::function<void (const xTcpconnectionPtr&, size_t)> HighWaterMarkCallback;
typedef std::function<void (const xTcpconnectionPtr&,xBuffer*)> MessageCallback;








