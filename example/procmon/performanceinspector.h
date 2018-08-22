#pragma once
#include "inspector.h"
#include "log.h"

namespace PerformanceInspector
{
	void registerCommands(Inspector *ins);

	static string heap(HttpRequest::Method, const Inspector::ArgList&);
	static string growth(HttpRequest::Method, const Inspector::ArgList&);
	static string profile(HttpRequest::Method, const Inspector::ArgList&);
	static string cmdline(HttpRequest::Method, const Inspector::ArgList&);
	static string memstats(HttpRequest::Method, const Inspector::ArgList&);
	static string memhistogram(HttpRequest::Method, const Inspector::ArgList&);
	static string releaseFreeMemory(HttpRequest::Method, const Inspector::ArgList&);
	static string symbol(HttpRequest::Method, const Inspector::ArgList&);
};
