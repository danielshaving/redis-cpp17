#include "processinfo.h"

ReadSmallFile::ReadSmallFile(const std::string_view  &filename)
	: fd(::open(filename.data(), O_RDONLY | O_CLOEXEC)),
	err(0)
{
	buf[0] = '\0';
	if (fd < 0)
	{
		err = errno;
	}
}

ReadSmallFile::~ReadSmallFile()
{
	if (fd >= 0)
	{
		::close(fd); // FIXME: check EINTR
	}
}

// return errno
int32_t ReadSmallFile::readToString(int32_t maxSize,
	std::string *content,
	int64_t *fileSize,
	int64_t *modifyTime,
	int64_t *createTime)
{
	static_assert(sizeof(off_t) == 8, "_FILE_OFFSET_BITS = 64");
	assert(content != NULL);
	if (fd >= 0)
	{
		content->clear();

		if (fileSize)
		{
			struct stat statbuf;
			if (::fstat(fd, &statbuf) == 0)
			{
				if (S_ISREG(statbuf.st_mode))
				{
					*fileSize = statbuf.st_size;
					content->reserve(maxSize);
				}
				else if (S_ISDIR(statbuf.st_mode))
				{
					err = EISDIR;
				}

				if (modifyTime)
				{
					*modifyTime = statbuf.st_mtime;
				}

				if (createTime)
				{
					*createTime = statbuf.st_ctime;
				}
			}
			else
			{
				err = errno;
			}
		}

		while (content->size() < maxSize)
		{
			size_t toRead = std::min(maxSize - content->size(), sizeof(buf));
			ssize_t n = ::read(fd, buf, toRead);
			if (n > 0)
			{
				content->append(buf, n);
			}
			else
			{
				if (n < 0)
				{
					err = errno;
				}
				break;
			}
		}
	}
	return err;
}

int32_t ReadSmallFile::readToBuffer(int32_t *size)
{
	if (fd >= 0)
	{
		ssize_t n = ::pread(fd, buf, sizeof(buf) - 1, 0);
		if (n >= 0)
		{
			if (size)
			{
				*size = static_cast<int32_t>(n);
			}
			buf[n] = '\0';
		}
		else
		{
			err = errno;
		}
	}
	return err;
}

__thread int t_numOpenedFiles = 0;
int fdDirFilter(const struct dirent* d)
{
	if (::isdigit(d->d_name[0]))
	{
		++t_numOpenedFiles;
	}
	return 0;
}

__thread std::vector<pid_t>* t_pids = nullptr;
int taskDirFilter(const struct dirent* d)
{
	if (::isdigit(d->d_name[0]))
	{
		t_pids->push_back(atoi(d->d_name));
	}
	return 0;
}

int scanDir(const char *dirpath, int(*filter)(const struct dirent *))
{
	struct dirent** namelist = nullptr;
	int result = ::scandir(dirpath, &namelist, filter, alphasort);
	assert(namelist == nullptr);
	return result;
}

TimeStamp g_startTime = TimeStamp::now();
// assume those won't change during the life time of a process.
int g_clockTicks = static_cast<int>(::sysconf(_SC_CLK_TCK));
int g_pageSize = static_cast<int>(::sysconf(_SC_PAGE_SIZE));

pid_t ProcessInfo::pid()
{
	return ::getpid();
}

std::string ProcessInfo::pidString()
{
	char buf[32];
	snprintf(buf, sizeof buf, "%d", pid());
	return buf;
}

uid_t ProcessInfo::uid()
{
	return ::getuid();
}

std::string ProcessInfo::username()
{
	struct passwd pwd;
	struct passwd* result = nullptr;
	char buf[8192];
	const char* name = "unknownuser";

	getpwuid_r(uid(), &pwd, buf, sizeof buf, &result);
	if (result)
	{
		name = pwd.pw_name;
	}
	return name;
}

uid_t ProcessInfo::euid()
{
	return ::geteuid();
}

TimeStamp ProcessInfo::startTime()
{
	return g_startTime;
}

int ProcessInfo::clockTicksPerSecond()
{
	return g_clockTicks;
}

int ProcessInfo::pageSize()
{
	return g_pageSize;
}

bool ProcessInfo::isDebugBuild()
{
#ifdef NDEBUG
	return false;
#else
	return true;
#endif
}

std::string ProcessInfo::hostname()
{
	// HOST_NAME_MAX 64
	// _POSIX_HOST_NAME_MAX 255
	char buf[256];
	if (::gethostname(buf, sizeof buf) == 0)
	{
		buf[sizeof(buf) - 1] = '\0';
		return buf;
	}
	else
	{
		return "unknownhost";
	}
}

std::string ProcessInfo::procname()
{
	std::string_view view = procname(procStat());
	return std::string(view.data(), view.size());
}

std::string_view ProcessInfo::procname(const std::string& stat)
{
	std::string_view name;
	size_t lp = stat.find('(');
	size_t rp = stat.rfind(')');
	if (lp != std::string::npos && rp != std::string::npos && lp < rp)
	{
		name = std::string_view(stat.data() + lp + 1, rp - lp - 1);
	}
	return name;
}

std::string ProcessInfo::procStatus()
{
	ReadSmallFile file("/proc/self/stat");
	std::string result;
	file.readToString(65536, &result);
	return result;
}

std::string ProcessInfo::procStat()
{
	ReadSmallFile file("/proc/self/stat");
	std::string result;
	file.readToString(65536, &result);
	return result;
}

std::string ProcessInfo::threadStat()
{
	char buf[64];
	snprintf(buf, sizeof buf, "/proc/self/task/%d/stat", std::this_thread::get_id());
	ReadSmallFile file(buf);
	std::string result;
	file.readToString(65536, &result);
	return result;
}

std::string ProcessInfo::exePath()
{
	std::string result;
	char buf[1024];
	ssize_t n = ::readlink("/proc/self/exe", buf, sizeof buf);
	if (n > 0)
	{
		result.assign(buf, n);
	}
	return result;
}

int ProcessInfo::openedFiles()
{
	t_numOpenedFiles = 0;
	scanDir("/proc/self/fd", fdDirFilter);
	return t_numOpenedFiles;
}

int ProcessInfo::maxOpenFiles()
{
	struct rlimit rl;
	if (::getrlimit(RLIMIT_NOFILE, &rl))
	{
		return openedFiles();
	}
	else
	{
		return static_cast<int>(rl.rlim_cur);
	}
}

ProcessInfo::CpuTime ProcessInfo::cpuTime()
{
	ProcessInfo::CpuTime t;
	struct tms tms;
	if (::times(&tms) >= 0)
	{
		const double hz = static_cast<double>(clockTicksPerSecond());
		t.userSeconds = static_cast<double>(tms.tms_utime) / hz;
		t.systemSeconds = static_cast<double>(tms.tms_stime) / hz;
	}
	return t;
}

int ProcessInfo::numThreads()
{
	int result = 0;
	std::string status = procStatus();
	size_t pos = status.find("Threads:");
	if (pos != std::string::npos)
	{
		result = ::atoi(status.c_str() + pos + 8);
	}
	return result;
}

std::vector<pid_t> ProcessInfo::threads()
{
	std::vector<pid_t> result;
	t_pids = &result;
	scanDir("/proc/self/task", taskDirFilter);
	t_pids = nullptr;
	std::sort(result.begin(), result.end());
	return result;
}


/*
	printf("pid = %d\n", PerformanceInspector::pid());
	printf("uid = %d\n", PerformanceInspector::uid());
	printf("euid = %d\n", PerformanceInspector::euid());
	printf("start time = %s\n", PerformanceInspector::startTime().toFormattedString().c_str());
	printf("hostname = %s\n", PerformanceInspector::hostname().c_str());
	printf("opened files = %d\n", PerformanceInspector::openedFiles());
	printf("threads = %zd\n", PerformanceInspector::threads().size());
	printf("num threads = %d\n", PerformanceInspector::numThreads());
	printf("status = %s\n", PerformanceInspector::procStatus().c_str());
*/
