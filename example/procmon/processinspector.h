#pragma once
#include "inspector.h"

class ProcessInspector
{
public:
	void registerCommands(Inspector *ins);

	static std::string overview(HttpRequest::Method, const Inspector::ArgList&);
	static std::string pid(HttpRequest::Method, const Inspector::ArgList&);
	static std::string procStatus(HttpRequest::Method, const Inspector::ArgList&);
	static std::string openedFiles(HttpRequest::Method, const Inspector::ArgList&);
	static std::string threads(HttpRequest::Method, const Inspector::ArgList&);

	static std::string username_;
};
