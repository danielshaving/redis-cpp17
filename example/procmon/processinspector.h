#pragma once
#include "inspector.h"

class ProcessInspector
{
public:
	void registerCommands(Inspector* ins);

	static string overview(HttpRequest::Method, const Inspector::ArgList&);
	static string pid(HttpRequest::Method, const Inspector::ArgList&);
	static string procStatus(HttpRequest::Method, const Inspector::ArgList&);
	static string openedFiles(HttpRequest::Method, const Inspector::ArgList&);
	static string threads(HttpRequest::Method, const Inspector::ArgList&);

	static string username_;
};
