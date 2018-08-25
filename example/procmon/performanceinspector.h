#pragma once
#include "inspector.h"
#include "log.h"

class PerformanceInspector
{
public:
	void registerCommands(Inspector *ins);

	static std::string heap(HttpRequest::Method, const Inspector::ArgList&);
	static std::string growth(HttpRequest::Method, const Inspector::ArgList&);
	static std::string profile(HttpRequest::Method, const Inspector::ArgList&);
	static std::string cmdline(HttpRequest::Method, const Inspector::ArgList&);
	static std::string memstats(HttpRequest::Method, const Inspector::ArgList&);
	static std::string memhistogram(HttpRequest::Method, const Inspector::ArgList&);
	static std::string releaseFreeMemory(HttpRequest::Method, const Inspector::ArgList&);
	static std::string symbol(HttpRequest::Method, const Inspector::ArgList&);
};
