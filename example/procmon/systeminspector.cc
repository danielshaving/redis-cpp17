#include "systeminspector.h"
#include "processinfo.h"

std::string uptime(TimeStamp now, TimeStamp start, bool showMicroseconds);
long getLong(const std::string& content, const char* key);
int stringPrintf(std::string* out, const char* fmt, ...) __attribute__((format(printf, 2, 3)));


void SystemInspector::registerCommands(Inspector* ins)
{
	ins->add("sys", "overview", SystemInspector::overview, "print system overview");
	ins->add("sys", "loadavg", SystemInspector::loadavg, "print /proc/loadavg");
	ins->add("sys", "version", SystemInspector::version, "print /proc/version");
	ins->add("sys", "cpuinfo", SystemInspector::cpuinfo, "print /proc/cpuinfo");
	ins->add("sys", "meminfo", SystemInspector::meminfo, "print /proc/meminfo");
	ins->add("sys", "stat", SystemInspector::stat, "print /proc/stat");
}

std::string SystemInspector::loadavg(HttpRequest::Method, const Inspector::ArgList&)
{
	ReadSmallFile file("/proc/loadavg");
	std::string loadavg;
	file.readToString(65536, &loadavg);
	return loadavg;
}

std::string SystemInspector::version(HttpRequest::Method, const Inspector::ArgList&)
{
	ReadSmallFile file("/proc/version");
	std::string version;
	file.readToString(65536, &version);
	return version;
}

std::string SystemInspector::cpuinfo(HttpRequest::Method, const Inspector::ArgList&)
{
	ReadSmallFile file("/proc/cpuinfo");
	std::string cpuinfo;
	file.readToString(65536, &cpuinfo);
	return cpuinfo;
}

std::string SystemInspector::meminfo(HttpRequest::Method, const Inspector::ArgList&)
{
	ReadSmallFile file("/proc/meminfo");
	std::string meminfo;
	file.readToString(65536, &meminfo);
	return meminfo;
}

std::string SystemInspector::stat(HttpRequest::Method, const Inspector::ArgList&)
{
	ReadSmallFile file("/proc/stat");
	std::string stat;
	file.readToString(65536, &stat);
	return stat;
}

std::string SystemInspector::overview(HttpRequest::Method, const Inspector::ArgList&)
{
	std::string result;
	result.reserve(1024);
	TimeStamp now = TimeStamp::now();
	result += "Page generated at ";
	result += now.toFormattedString();
	result += " (UTC)\n";
	// Hardware and OS
	{
#ifdef __linux__
		struct utsname un;
		if (::uname(&un) == 0)
		{
			stringPrintf(&result, "Hostname: %s\n", un.nodename);
			stringPrintf(&result, "Machine: %s\n", un.machine);
			stringPrintf(&result, "OS: %s %s %s\n", un.sysname, un.release, un.version);
		}
#endif
	}
	std::string stat;
	ReadSmallFile file("/proc/stat");
	file.readToString(65536, &stat);
	TimeStamp bootTime(TimeStamp::kMicroSecondsPerSecond * getLong(stat, "btime "));
	result += "Boot time: ";
	result += bootTime.toFormattedString(false /* show microseconds */);
	result += " (UTC)\n";
	result += "Up time: ";
	result += uptime(now, bootTime, false /* show microseconds */);
	result += "\n";

	// CPU load
	{
		ReadSmallFile file("/proc/loadavg");
		std::string loadavg;
		file.readToString(65536, &loadavg);
		stringPrintf(&result, "Processes created: %ld\n", getLong(stat, "processes "));
		stringPrintf(&result, "Loadavg: %s\n", loadavg.c_str());
	}

	// Memory
	{
		ReadSmallFile file("/proc/meminfo");
		std::string meminfo;
		file.readToString(65536, &meminfo);
		long total_kb = getLong(meminfo, "MemTotal:");
		long free_kb = getLong(meminfo, "MemFree:");
		long buffers_kb = getLong(meminfo, "Buffers:");
		long cached_kb = getLong(meminfo, "Cached:");

		stringPrintf(&result, "Total Memory: %6ld MiB\n", total_kb / 1024);
		stringPrintf(&result, "Free Memory:  %6ld MiB\n", free_kb / 1024);
		stringPrintf(&result, "Buffers:      %6ld MiB\n", buffers_kb / 1024);
		stringPrintf(&result, "Cached:       %6ld MiB\n", cached_kb / 1024);
		stringPrintf(&result, "Real Used:    %6ld MiB\n", (total_kb - free_kb - buffers_kb - cached_kb) / 1024);
		stringPrintf(&result, "Real Free:    %6ld MiB\n", (free_kb + buffers_kb + cached_kb) / 1024);

		// Swap
	}
	// Disk
	// Network
	return result;
}
