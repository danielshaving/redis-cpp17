#include "httpcurl.h"


EventLoop* g_loop = NULL;

void onData(Request* c, const char* data, int len)
{
	printf("data %s\n", data);
}

void onHead(const char* data, int len)
{
	printf("data1 %s\n", data);
}

void done(Request* c, int code)
{
    printf("done %p %s %d\n", c, c->getEffectiveUrl(), code);
}

void done2(Request* c, int code)
{
	printf("done2 %p %s %d %d\n", c, c->getRedirectUrl(), c->getResponseCode(), code);
  //g_loop->quit();
}

int main(int argc, char* argv[])
{
	EventLoop loop;
	g_loop = &loop;
	loop.runAfter(5.0, std::bind(&EventLoop::quit, &loop));
	Curl::initialize(Curl::kCURLnossl);
	Curl curl(&loop);
	signal(SIGPIPE, SIG_IGN);

	RequestPtr req = curl.getUrl("http://192.168.6.1:8000/");
	req->setDataCallback(onData);
	req->setDoneCallback(done2);
	req->setHeaderCallback(onHead);

	curl::RequestPtr req2 = curl.getUrl("https://github.com");
	//req2->allowRedirect(5);
	req2->setDataCallback(onData);
	req2->setDoneCallback(done);
	loop.run();
}