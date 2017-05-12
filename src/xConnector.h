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

  xConnector(xEventLoop* loop, std::string ip,int port);
  ~xConnector();

  void setNewConnectionCallback(const NewConnectionCallback& cb)
  {
	  newConnectionCallback = cb;
  }

  void setConnectionErrorCallBack(const ErrorConnectionCallback & cb)
  {
	  errorConnectionCallback = cb;
  }

  void start();
  void restart();
  void stop();

  void startInLoop();
  void stopInLoop();
  void connect();
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
  boost::scoped_ptr<xChannel> channel;
  bool isconnect;
  ErrorConnectionCallback	errorConnectionCallback;
  NewConnectionCallback newConnectionCallback;
  xSocket    socket;
};
