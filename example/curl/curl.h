#pragma once
#include "all.h"
#include "eventloop.h"
#include "channel.h"
#include "tcpserver.h"
#include "log.h"
#include <curl/curl.h>

extern "C"
{
	typedef void CURLM;
	typedef void CURL;
}

class Curl;
class Request : public std::enable_shared_from_this<Request>
{
public:
	typedef std::function<void(Request *, const char *, int)> DataCallback;
	typedef std::function<void(const char *, int)> HeadCallback;
	typedef std::function<void(Request *, int)> DoneCallback;

	Request(Curl *, const char *url);
	Request(Curl *, const char *url, const char *data, size_t len);
	
	~Request();

	void setDataCallback(const DataCallback &cb)
	{ dataCb = cb; }

	void setDoneCallback(const DoneCallback &cb)
	{ doneCb = cb; }

	void setHeaderCallback(const HeadCallback &cb)
	{ headerCb = cb; }

	void headerOnly();

	void setRange(const std::string_view range);

	template<typename OPT>
	int setopt(OPT opt, long p)
	{
		return curl_easy_setopt(curl, opt, p);
	}

	template<typename OPT>
	int setopt(OPT opt, const char *p)
	{
		return curl_easy_setopt(curl, opt, p);
	}

	template<typename OPT>
	int setopt(OPT opt, void *p)
	{
		return curl_easy_setopt(curl, opt, p);
	}

	template<typename OPT>
	int setopt(OPT opt, size_t (*p)(char *, size_t , size_t , void *))
	{
		return curl_easy_setopt(curl, opt, p);
	}

	TcpConnectionPtr getTcpConnection() { return conn; }
	void setTcpConnection(const TcpConnectionPtr &connection) { conn = connection; }
	const char *getEffectiveUrl();
	const char *getRedirectUrl();
	int getResponseCode();

	// internal
	Channel *setChannel(int fd);
	void removeChannel();
	void done(int code);
	CURL *getCurl() { return curl; }
	Channel *getChannel() { return channel.get(); }
private:
	Request(const Request&);
	void operator=(const Request&);

	void dataCallback(const char *buffer, int len);
	void headerCallback(const char *buffer, int len);
	static size_t writeData(char *buffer, size_t size, size_t nmemb, void *userp);
	static size_t headerData(char *buffer, size_t size, size_t nmemb, void *userp);
	void doneCallback();

	class Curl *owner;
	CURL *curl;
	std::shared_ptr<Channel> channel;
	DataCallback dataCb;
	HeadCallback headerCb;
	DoneCallback doneCb;
	TcpConnectionPtr conn;
};

typedef std::shared_ptr<Request> RequestPtr;
class Curl
{
public:
	enum Option
	{
		kCURLnossl = 0,
		kCURLssl   = 1,
	};

	explicit Curl(EventLoop *loop);
	~Curl();

	RequestPtr getUrl(std::string_view url);
	RequestPtr getUrl(std::string_view url, std::string_view body);

	static void initialize(Option opt = kCURLnossl);

	// internal
	CURLM *getCurlm() { return curlm; }
	EventLoop* getLoop() { return loop; }

 private:
	Curl(const Curl&);
	void operator=(const Curl&);

	void onTimer();
	void onRead(int fd);
	void onWrite(int fd);
	void checkFinish();

	static int socketCallback(CURL *, int, int, void*, void*);
	static int timerCallback(CURLM *, long, void*);

	EventLoop *loop;
	CURLM *curlm;
	int runningHandles;
	int prevRunningHandles;
};

