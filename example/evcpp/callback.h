#pragma once

namespace evcpp
{
	typedef std::function<void()> Functor;
	typedef std::function<void()> TimerCallback;

	class TcpConnection;
	typedef std::shared_ptr<TcpConnection> TcpConnectionPtr;
	typedef std::function<void(TcpConnectionPtr)> NewConnectionCallback;
}
