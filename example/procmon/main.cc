#include "eventloop.h"
#include "inspector.h"

int main()
{
	EventLoop loop;
	Inspector ins(&loop,"127.0.0.1",8888,"test");
	loop.run();
}
