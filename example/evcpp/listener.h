#pragma once

namespace evcpp
{
	class Listener
	{
	public:
		Listener(EventLoop *loop,int port)
		:listener(evconnlistener_new_bind(
			  loop->eventBase(),
			  newConnectionCallback,
			  this,
			  LEV_OPT_CLOSE_ON_FREE | LEV_OPT_CLOSE_ON_EXEC | LEV_OPT_REUSEABLE,
			  -1,
			  getListenSock(port),
			  sizeof(struct sockaddr_in)))
		{
			assert(listener != nullptr);
		}

		~Listener()
		{
			::evconnlistener_free(listener);
		}

		void setNewConnectionCallback(const NewConnectionCallback &cb) { callback = cb; }

	private:
		void onConnect(evutil_socket_t fd,const struct sockaddr_in *address)
		{
			if (callback)
			{
				TcpConnectionPtr conn(std::make_shared<TcpConnection>(
				evconnlistener_get_base(listener), fd));
				callback(std::move(conn));
			}
			else
			{
				::close(fd);
			}
		}

		static struct sockaddr* getListenSock(int port)
		{
			struct sockaddr_in sin;
			sin.sin_family = AF_INET;
			sin.sin_addr.s_addr = INADDR_ANY;
			sin.sin_port = htons(port);
			return reinterpret_cast<struct sockaddr*>(&sin);
		}

		static void newConnectionCallback(struct evconnlistener *listener,
		  evutil_socket_t fd,struct sockaddr *address,int socklen,void *ctx)
		{
			Listener *self = static_cast<Listener*>(ctx);
			assert(self->listener == listener);
			assert(socklen == sizeof(struct sockaddr_in));
			self->onConnect(fd,reinterpret_cast<struct sockaddr_in*>(address));
		}

		struct evconnlistener *const listener;
		NewConnectionCallback callback;
	};
}


