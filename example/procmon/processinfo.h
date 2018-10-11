#pragma once
#include "timerqueue.h"
#include "log.h"

// read small file < 64KB
class ReadSmallFile
{
public:
	ReadSmallFile(const std::string_view &filename);
	~ReadSmallFile();

	// return errno
	int32_t readToString(int32_t maxSize,
		std::string *content,
		int64_t *fileSize = nullptr,
		int64_t *modifyTime = nullptr,
		int64_t *createTime = nullptr);

	/// Read at maxium kBufferSize into buf_
	// return errno
	int32_t readToBuffer(int32_t *size);

	const char *buffer() const { return buf; }

	static const int32_t kBufferSize = 64 * 1024;

private:
	int32_t fd;
	int32_t err;
	char buf[kBufferSize];
};

namespace ProcessInfo
{
	pid_t pid();
	std::string pidString();
	uid_t uid();
	std::string username();
	uid_t euid();
	TimeStamp startTime();
	int clockTicksPerSecond();
	int pageSize();
	bool isDebugBuild();  // constexpr

	std::string hostname();
	std::string procname();
	std::string_view procname(const std::string &stat);

	/// read /proc/self/status
	std::string procStatus();

	/// read /proc/self/stat
	std::string procStat();

	/// read /proc/self/task/tid/stat
	std::string threadStat();

	/// readlink /proc/self/exe
	std::string exePath();

	int openedFiles();
	int maxOpenFiles();

	struct CpuTime
	{
		double userSeconds;
		double systemSeconds;

		CpuTime() : userSeconds(0.0), systemSeconds(0.0) { }
	};
	CpuTime cpuTime();

	int numThreads();
	std::vector<pid_t> threads();
};
