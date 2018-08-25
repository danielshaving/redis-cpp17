#pragma once
#include "httprequest.h"
#include "httpserver.h"

class ProcessInspector;
class PerformanceInspector;
class SystemInspector;

// An internal inspector of the running process, usually a singleton.
// Better to run in a seperated thread, as some method may block for seconds
class Inspector
{
public:
	typedef std::vector<std::string> ArgList;
	typedef std::function<std::string(HttpRequest::Method, const ArgList &args)> Callback;
	Inspector(EventLoop *loop,
		const char *ip, uint16_t port,
		const std::string &name);
	~Inspector();

	/// Add a Callback for handling the special uri : /mudule/command
	void add(const std::string &module,
		const std::string &command,
		const Callback &cb,
		const std::string &help);
	void remove(const std::string &module, const std::string &command);

private:
	typedef std::map<std::string, Callback> CommandList;
	typedef std::map<std::string, std::string> HelpList;

	void start();
	void onRequest(const HttpRequest &req, HttpResponse *resp);

	HttpServer server;
	std::unique_ptr<ProcessInspector> processInspector;
	std::unique_ptr<PerformanceInspector> performanceInspector;
	std::unique_ptr<SystemInspector> systemInspector;
	std::mutex mutex;
	std::map<std::string, CommandList> modules;
	std::map<std::string, HelpList> helps;
};
