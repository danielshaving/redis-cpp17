#include "evcpp.h"

void onConnect(evcpp::TcpConnectionPtr conn)
{
	printf("onConnect %zd\n",conn.use_count());
}

int main()
{
	evcpp::EventLoop loop;
	evcpp::Listener listener(&loop,1234);
	listener.setNewConnectionCallback(onConnect);
	loop.loop();
}
