#include "processinspector.h"
#include "processinfo.h"

std::string uptime(TimeStamp now, TimeStamp start, bool showMicroseconds)
{
	char buf[256];
	int64_t age = now.getMicroSecondsSinceEpoch() - start.getMicroSecondsSinceEpoch();
	int seconds = static_cast<int>(age / TimeStamp::kMicroSecondsPerSecond);
	int days = seconds / 86400;
	int hours = (seconds % 86400) / 3600;
	int minutes = (seconds % 3600) / 60;
	if (showMicroseconds)
	{
		int microseconds = static_cast<int>(age % TimeStamp::kMicroSecondsPerSecond);
		snprintf(buf, sizeof buf, "%d days %02d:%02d:%02d.%06d",
			days, hours, minutes, seconds % 60, microseconds);
	}
	else
	{
		snprintf(buf, sizeof buf, "%d days %02d:%02d:%02d",
			days, hours, minutes, seconds % 60);
	}
	return buf;
}

long getLong(const std::string &procStatus, const char *key)
{
	long result = 0;
	size_t pos = procStatus.find(key);
	if (pos != std::string::npos)
	{
		result = ::atol(procStatus.c_str() + pos + strlen(key));
	}
	return result;
}

std::string getProcessName(const std::string &procStatus)
{
	std::string result;
	size_t pos = procStatus.find("Name:");
	if (pos != std::string::npos)
	{
		pos += strlen("Name:");
		while (procStatus[pos] == '\t')
			++pos;
		size_t eol = pos;
		while (procStatus[eol] != '\n')
			++eol;
		result = procStatus.substr(pos, eol - pos);
	}
	return result;
}

std::string_view next(std::string_view data)
{
	const char* sp = static_cast<const char*>(::memchr(data.data(), ' ', data.size()));
	if (sp)
	{
		data.remove_prefix(static_cast<int>(sp + 1 - data.begin()));
		return data;
	}
	return "";
}

ProcessInfo::CpuTime getCpuTime(std::string_view data)
{
	ProcessInfo::CpuTime t;

	for (int i = 0; i < 10; ++i)
	{
		data = next(data);
	}
	long utime = strtol(data.data(), NULL, 10);
	data = next(data);
	long stime = strtol(data.data(), NULL, 10);
	const double hz = static_cast<double>(ProcessInfo::clockTicksPerSecond());
	t.userSeconds = static_cast<double>(utime) / hz;
	t.systemSeconds = static_cast<double>(stime) / hz;
	return t;
}

int stringPrintf(std::string *out, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

int stringPrintf(std::string *out, const char *fmt, ...)
{
	char buf[256];
	va_list args;
	va_start(args, fmt);
	int ret = vsnprintf(buf, sizeof buf, fmt, args);
	va_end(args);
	out->append(buf);
	return ret;
}

std::string ProcessInspector::username = ProcessInfo::username();

void ProcessInspector::registerCommands(Inspector *ins)
{
	ins->add("proc", "overview", ProcessInspector::overview, "print basic overview");
	ins->add("proc", "pid", ProcessInspector::pid, "print pid");
	ins->add("proc", "status", ProcessInspector::procStatus, "print /proc/self/status");
	// ins->add("proc", "opened_files", ProcessInspector::openedFiles, "count /proc/self/fd");
	ins->add("proc", "threads", ProcessInspector::threads, "list /proc/self/task");
}

std::string ProcessInspector::overview(HttpRequest::Method, const Inspector::ArgList&)
{
	std::string result;
	result.reserve(1024);
	TimeStamp now = TimeStamp::now();
	result += "Page generated at ";
	result += now.toFormattedString();
	result += " (UTC)\nStarted at ";
	result += ProcessInfo::startTime().toFormattedString();
	result += " (UTC), up for ";
	result += uptime(now, ProcessInfo::startTime(), true/* show microseconds */);
	result += "\n";

	std::string procStatus = ProcessInfo::procStatus();
	result += getProcessName(procStatus);
	result += " (";
	result += ProcessInfo::exePath();
	result += ") running as ";
	result += username;
	result += " on ";
	result += ProcessInfo::hostname(); // cache ?
	result += "\n";

	if (ProcessInfo::isDebugBuild())
	{
		result += "WARNING: debug build!\n";
	}

	stringPrintf(&result, "pid %d, num of threads %ld, bits %zd\n",
		ProcessInfo::pid(), getLong(procStatus, "Threads:"), CHAR_BIT * sizeof(void*));

	result += "Virtual memory: ";
	stringPrintf(&result, "%.3f MiB, ",
		static_cast<double>(getLong(procStatus, "VmSize:")) / 1024.0);

	result += "RSS memory: ";
	stringPrintf(&result, "%.3f MiB\n",
		static_cast<double>(getLong(procStatus, "VmRSS:")) / 1024.0);

	// FIXME: VmData:

	stringPrintf(&result, "Opened files: %d, limit: %d\n",
		ProcessInfo::openedFiles(), ProcessInfo::maxOpenFiles());

	// std::string procStat = ProcessInfo::procStat();

	/*
	stringPrintf(&result, "ppid %ld\n", getStatField(procStat, 0));
	stringPrintf(&result, "pgid %ld\n", getStatField(procStat, 1));
	*/

	ProcessInfo::CpuTime t = ProcessInfo::cpuTime();
	stringPrintf(&result, "User time: %12.3fs\nSys time:  %12.3fs\n",
		t.userSeconds, t.systemSeconds);

	// FIXME: add context switches

	return result;
}

std::string ProcessInspector::pid(HttpRequest::Method, const Inspector::ArgList&)
{
	char buf[32];
	snprintf(buf, sizeof buf, "%d", ProcessInfo::pid());
	return buf;
}

std::string ProcessInspector::procStatus(HttpRequest::Method, const Inspector::ArgList&)
{
	return ProcessInfo::procStatus();
}

std::string ProcessInspector::openedFiles(HttpRequest::Method, const Inspector::ArgList&)
{
	char buf[32];
	snprintf(buf, sizeof buf, "%d", ProcessInfo::openedFiles());
	return buf;
}

std::string ProcessInspector::threads(HttpRequest::Method, const Inspector::ArgList&)
{
	std::vector<pid_t> threads = ProcessInfo::threads();
	std::string result = "  TID NAME             S    User Time  System Time\n";
	result.reserve(threads.size() * 64);
	std::string stat;
	for (size_t i = 0; i < threads.size(); ++i)
	{
		char buf[256];
		int tid = threads[i];
		snprintf(buf, sizeof buf, "/proc/%d/task/%d/stat", ProcessInfo::pid(), tid);
		ReadSmallFile file(buf);
		if (file.readToString(65536, &stat) == 0)
		{
			std::string_view name = ProcessInfo::procname(stat);
			const char* rp = name.end();
			assert(*rp == ')');
			const char* state = rp + 2;
			*const_cast<char*>(rp) = '\0';  // don't do this at home
			std::string_view data(stat);
			data.remove_prefix(static_cast<int>(state - data.data() + 2));
			ProcessInfo::CpuTime t = getCpuTime(data);
			snprintf(buf, sizeof buf, "%5d %-16s %c %12.3f %12.3f\n",
				tid, name.data(), *state, t.userSeconds, t.systemSeconds);
			result += buf;
		}
	}
	return result;
}

