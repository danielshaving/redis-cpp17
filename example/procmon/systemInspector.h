#include "inspector.h"

class SystemInspector
{
public:
	void registerCommands(Inspector *ins);

	static std::string overview(HttpRequest::Method, const Inspector::ArgList&);
	static std::string loadavg(HttpRequest::Method, const Inspector::ArgList&);
	static std::string version(HttpRequest::Method, const Inspector::ArgList&);
	static std::string cpuinfo(HttpRequest::Method, const Inspector::ArgList&);
	static std::string meminfo(HttpRequest::Method, const Inspector::ArgList&);
	static std::string stat(HttpRequest::Method, const Inspector::ArgList&);
};

