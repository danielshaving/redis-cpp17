#pragma once

namespace evcpp
{
class TcpConnection : public std::enable_shared_from_this<TcpConnection>
{
	public:
	TcpConnection(struct event_base* base,int fd)
	: conn(::bufferevent_socket_new(base,fd,BEV_OPT_CLOSE_ON_FREE))
	{

	}

	~TcpConnection()
	{
		::bufferevent_free(conn);
	}

	private:
		struct bufferevent *const conn;
	};
}
