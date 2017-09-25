#pragma once
#include "all.h"
#include "xEventLoop.h"
#include "xChannel.h"
#include "xSocket.h"
#include "xLog.h"

class xConnector
{
public:
  typedef std::function<void (int sockfd)> NewConnectionCallback;
  typedef std::function<void()> ErrorConnectionCallback;

  xConnector(xEventLoop* loop);
  ~xConnector();

  void setNewConnectionCallback(const NewConnectionCallback& cb)
  {
	  newConnectionCallback = cb;
  }

  void setConnectionErrorCallBack(const ErrorConnectionCallback & cb)
  {
	  errorConnectionCallback = cb;
  }

  void start(const char *ip, int16_t port);
  void restart();
  void stop();

  void startInLoop(const char *ip, int16_t port);
  void stopInLoop();
  void connect(const char *ip, int16_t port);
  void connecting(int sockfd);
  void resetChannel();
  int  removeAndResetChannel();

private:
  enum States { kDisconnected, kConnecting, kConnected };
public:
  void setState(States  s) { state = s; }
  xEventLoop* loop;
  std::string ip;
  int port;

  States state;
  std::unique_ptr<xChannel> channel;
  bool isconnect;
  ErrorConnectionCallback	errorConnectionCallback;
  NewConnectionCallback newConnectionCallback;
  xSocket    socket;
};
