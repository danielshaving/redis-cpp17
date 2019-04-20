#include "log.h"

const char digits[] = "9876543210123456789";
const char digitsHex[] = "0123456789ABCDEF";
const char *zero = digits + 9;
char ttime[32];

template<typename T>
size_t convert(char buf[], T value) {
	T i = value;
	char *p = buf;

	do {
		int32_t lsd = static_cast<int32_t>(i % 10);
		i /= 10;
		*p++ = zero[lsd];
	} while (i != 0);

	if (value < 0) {
		*p++ = '-';
	}

	*p = '\0';
	std::reverse(buf, p);
	return p - buf;
}

size_t convertHex(char buf[], uintptr_t value) {
	uintptr_t i = value;
	char *p = buf;

	do {
		int32_t lsd = static_cast<int32_t>(i % 16);
		i /= 16;
		*p++ = digitsHex[lsd];
	} while (i != 0);

	*p = '\0';
	std::reverse(buf, p);
	return p - buf;
}

AppendFile::AppendFile(const std::string &filename)
	: filename(filename),
	writtenBytes(0) {
#ifdef _WIN64
	fp = ::fopen(filename.c_str(), "at+");
#else
	fp = ::fopen(filename.c_str(), "ae"); // 'e' for O_CLOEXEC
#endif
	assert(fp);
}

AppendFile::~AppendFile() {
	::fclose(fp);
}

bool AppendFile::exists(const std::string &filename) {
#ifndef __APPLE__
	return std::experimental::filesystem::exists(filename.c_str());
#endif
}

bool AppendFile::lockFile(const std::string &fname) {
#ifndef _WIN64
	int32_t fd = ::open(fname.c_str(), O_RDWR | O_CREAT, 0644);
	if (fd < 0) {
		return false;
	}
	else if (lockOrUnlock(fd, true) == -1) {
		return false;
	}
	else {
		this->fd = fd;
	}
#endif
	return true;
}

bool AppendFile::unlockFile() {
#ifndef _WIN64
	if (lockOrUnlock(fd, false) == -1) {
		return false;
	}
	::close(fd);
#endif
	return true;
}

int32_t AppendFile::lockOrUnlock(int32_t fd, bool lock) {
#ifndef _WIN64
	errno = 0;
	struct flock f;
	memset(&f, 0, sizeof(f));
	f.l_type = (lock ? F_WRLCK : F_UNLCK);
	f.l_whence = SEEK_SET;
	f.l_start = 0;
	f.l_len = 0;        // Lock/unlock entire file
	return ::fcntl(fd, F_SETLK, &f);
#else
	return 0;
#endif
}

void AppendFile::append(const char *logline, const size_t len) {
	size_t n = write(logline, len);
	size_t remain = len - n;
	while (remain > 0) {
		size_t x = write(logline + n, remain);
		if (x == 0) {
			int32_t err = ferror(fp);
			if (err) {
				LOG_WARN << "AppendFile append failed:" << strerror(err);
			}
			break;
		}

		n += x;
		remain = len - n;
	}
	writtenBytes += len;
}

void AppendFile::flush() {
	::fflush(fp);
}

size_t AppendFile::write(const char *logline, size_t len) {
#ifdef __linux__
	return ::fwrite_unlocked(logline, 1, len, fp);
#else
	return ::fwrite(logline, 1, len, fp);
#endif
}

LogFile::LogFile(const std::string &filePath, const std::string &basename,
	size_t rollSize,
	bool threadSafe,
	int32_t interval,
	int32_t checkEveryN)
	: filePath(filePath),
	basename(basename),
	rollSize(rollSize),
	interval(interval),
	checkEveryN(checkEveryN),
	count(0),
	startOfPeriod(0),
	lastRoll(0),
	lastFlush(0) {
	assert(basename.find('/') == std::string::npos);
#ifdef __APPLE__
	::mkdir(filePath.c_str(), 0755);
#else
	std::experimental::filesystem::create_directories(filePath);
#endif
	rollFile(true);
}

LogFile::~LogFile() {

}

void LogFile::append(const char *logline, int32_t len) {
	std::unique_lock <std::mutex> lk(mutex);
	appendUnlocked(logline, len);
}

void LogFile::flush() {
	std::unique_lock <std::mutex> lk(mutex);
	file->flush();
}

void LogFile::appendUnlocked(const char *logline, int32_t len) {
	/*
	std::experimental::filesystem::space_info info = std::experimental::filesystem::space(filePath.c_str());
	if (info.free <= rollSize * interval) {
		std::cout << ".        Capacity       Free      Available\n"
			<< ":   " << info.capacity << "   "
			<< info.free << "   " << info.available << '\n';
		return;
	}
	*/
	
	if (!file->exists(filePath + basename + ".log")) {
#ifdef __APPLE__
		::mkdir(filePath.c_str(), 0755);
#else
		std::experimental::filesystem::create_directories(filePath);
#endif
		filename.clear();
		count = 0;
		startOfPeriod = 0;
		lastRoll = 0;
		lastFlush = 0;
		rollFile(true);
		file->append(logline, len);
		return;
	}
	
	file->append(logline, len);
	if (file->getWrittenBytes() > rollSize) {
		rollFile();
	}
	else {
		++count;
		if (count >= checkEveryN) {
			count = 0;
			time_t now = ::time(0);
			time_t thisPeriod = now / kRollPerSeconds * kRollPerSeconds;
			if (thisPeriod != startOfPeriod) {
				rollFile();
			}
			else if (now - lastFlush > interval) {
				lastFlush = now;
				file->flush();
			}
		}
	}
}

bool LogFile::rollFile(bool mask) {
	time_t now = 0;
	getLogFileName(&now);
	time_t start = now / kRollPerSeconds * kRollPerSeconds;

	if (now > lastRoll) {		
		if (!mask) {
			if (!filename.empty()) {
				file.reset();
#ifdef __APPLE__
				std::string name = filePath + basename + ".log";
				std::rename(name.c_str(), filename.c_str());
#else
				std::experimental::filesystem::rename(filePath + basename + ".log", filename);
#endif
			}
		}
	
		lastRoll = now;
		lastFlush = now;
		startOfPeriod = start;
		file.reset(new AppendFile(filePath + basename + ".log"));
		return true;
	}
	return false;
}

void LogFile::getLogFileName(time_t *now) {
	filename.clear();
	filename += filePath;
	filename += basename;

	char timebuf[32];
	struct tm tm;
	*now = time(0);
	tm = *(localtime(now));
	strftime(timebuf, sizeof timebuf, ".%Y-%m-%d-%H-%M-%S", &tm);
	filename += timebuf;
	filename += ".log";
}

AsyncLogging::AsyncLogging(std::string filePath, std::string baseName, size_t rollSize, int32_t interval)
	: filePath(filePath),
	baseName(baseName),
	interval(interval),
	running(false),
	rollSize(rollSize),
	currentBuffer(new Buffer),
	nextBuffer(new Buffer),
	buffers() {
	currentBuffer->bzero();
	nextBuffer->bzero();
	buffers.reserve(16);
}

void AsyncLogging::append(const char *logline, size_t len) {
	std::unique_lock <std::mutex> lk(mutex);
	if (currentBuffer->avail() > len) {
		currentBuffer->append(logline, len);
	}
	else {
		buffers.push_back(std::unique_ptr<Buffer>(currentBuffer.release()));
		if (nextBuffer) {
			currentBuffer = std::move(nextBuffer);
		}
		else {
			currentBuffer.reset(new Buffer);
		}
		currentBuffer->append(logline, len);
		condition.notify_one();
	}
}

void AsyncLogging::threadFunc() {
	LogFile output(filePath, baseName, rollSize, false);
	BufferPtr newBuffer1(new Buffer);
	BufferPtr newBuffer2(new Buffer);
	newBuffer1->bzero();
	newBuffer2->bzero();

	BufferVector buffersToWrite;
	buffersToWrite.reserve(16);

	running = true;

	while (running) {
		assert(newBuffer1 && newBuffer1->length() == 0);
		assert(newBuffer2 && newBuffer2->length() == 0);
		assert(buffersToWrite.empty());
		{
			std::unique_lock <std::mutex> lk(mutex);
			if (buffers.empty()) {
				condition.wait_for(lk, std::chrono::seconds(interval));
			}

			buffers.push_back(std::unique_ptr<Buffer>(currentBuffer.release()));
			currentBuffer = std::move(newBuffer1);
			buffersToWrite.swap(buffers);

			if (!nextBuffer) {
				nextBuffer = std::move(newBuffer2);
			}
		}

		assert(!buffersToWrite.empty());

		if (buffersToWrite.size() > 25) {
			char buf[256];
			snprintf(buf, sizeof buf, "Dropped log messages at %s, %zd larger buffers\n",
				TimeStamp::now().toFormattedString().c_str(), buffersToWrite.size() - 2);
			fputs(buf, stderr);
			output.append(buf, static_cast<int>(::strlen(buf)));
			buffersToWrite.erase(buffersToWrite.begin() + 2, buffersToWrite.end());
		}

		for (size_t i = 0; i < buffersToWrite.size(); ++i) {
			output.append(buffersToWrite[i]->getData(), buffersToWrite[i]->length());
		}

		if (buffersToWrite.size() > 2) {
			buffersToWrite.resize(2);
		}

		if (!newBuffer1) {
			assert(!buffersToWrite.empty());
			newBuffer1 = std::move(buffersToWrite.back());
			buffersToWrite.pop_back();
			newBuffer1->reset();
		}

		if (!newBuffer2) {
			assert(!buffersToWrite.empty());
			newBuffer2 = std::move(buffersToWrite.back());
			buffersToWrite.pop_back();
			newBuffer2->reset();
		}

		buffersToWrite.clear();
		output.flush();
	}
	output.flush();
}

template<typename T>
void LogStream::formatInteger(T v) {
	if (buffer.avail() >= kMaxNumericSize) {
		size_t len = convert(buffer.current(), v);
		buffer.add(len);
	}
}

LogStream &LogStream::operator<<(short v) {
	*this << static_cast<int>(v);
	return *this;
}

LogStream &LogStream::operator<<(unsigned short v) {
	*this << static_cast<unsigned int>(v);
	return *this;
}

LogStream &LogStream::operator<<(int v) {
	formatInteger(v);
	return *this;
}

LogStream &LogStream::operator<<(unsigned int v) {
	formatInteger(v);
	return *this;
}

LogStream &LogStream::operator<<(long v) {
	formatInteger(v);
	return *this;
}

LogStream &LogStream::operator<<(unsigned long v) {
	formatInteger(v);
	return *this;
}

LogStream &LogStream::operator<<(long long v) {
	formatInteger(v);
	return *this;
}

LogStream &LogStream::operator<<(unsigned long long v) {
	formatInteger(v);
	return *this;
}

LogStream &LogStream::operator<<(const void *p) {
	uintptr_t v = reinterpret_cast<uintptr_t>(p);
	if (buffer.avail() >= kMaxNumericSize) {
		char *buf = buffer.current();
		buf[0] = '0';
		buf[1] = 'x';
		size_t len = convertHex(buf + 2, v);
		buffer.add(len + 2);
	}
	return *this;
}

// FIXME: replace this with Grisu3 by Florian Loitsch.
LogStream &LogStream::operator<<(double v) {
	if (buffer.avail() >= kMaxNumericSize) {
		int32_t len = snprintf(buffer.current(), kMaxNumericSize, "%.12g", v);
		buffer.add(len);
	}
	return *this;
}

Logger::LogLevel initLogLevel() {
#ifdef _WIN64
	return Logger::INFO;
#else
	if (::getenv("TRACE"))
		return Logger::TRACE;
	else if (::getenv("DEBUG"))
		return Logger::DEBUG;
	else
		return Logger::INFO;
#endif
}

Logger::LogLevel g_logLevel = initLogLevel();

const char *LogLevelName[Logger::NUM_LOG_LEVELS] = {
	"TRACE ",
	"DEBUG ",
	"INFO  ",
	"WARN  ",
	"ERRORR ",
	"FATAL ",
};

void defaultOutput(const char *msg, int32_t len) {
	size_t n = ::fwrite(msg, 1, len, stdout);
	//FIXME check n
	(void)n;
}

void defaultFlush() {
	fflush(stdout);
}

Logger::OutputFunc g_output = defaultOutput;
Logger::FlushFunc g_flush = defaultFlush;

Logger::Impl::Impl(LogLevel level, int32_t savedErrno, const SourceFile &file, int32_t line)
	: stream(),
	level(level),
	line(line),
	baseName(file),
	time(TimeStamp::now()) {
	formatTime();
	stream << T(LogLevelName[level], 6);
}

char t_time[64];
time_t t_lastSecond;

void Logger::Impl::formatTime() {
	int32_t len = 0;
	int64_t microSecondsSinceEpoch = time.getMicroSecondsSinceEpoch();
	time_t seconds = static_cast<time_t>(microSecondsSinceEpoch / TimeStamp::kMicroSecondsPerSecond);
	int microseconds = static_cast<int>(microSecondsSinceEpoch % TimeStamp::kMicroSecondsPerSecond);
	if (seconds != t_lastSecond) {
		t_lastSecond = seconds;
		struct tm tmtime = *(localtime(&seconds));
		len = snprintf(t_time, sizeof(t_time), "%4d%02d%02d %02d:%02d:%02d",
			tmtime.tm_year + 1900, tmtime.tm_mon + 1, tmtime.tm_mday,
			tmtime.tm_hour, tmtime.tm_min, tmtime.tm_sec);
		assert(len == 17);
		(void)len;
	}

	stream << T(t_time, 17);
	stream << "  ";
}

void Logger::Impl::finish() {
	stream << " - " << baseName.data << ':' << line << '\n';
}

Logger::Logger(SourceFile file, int32_t line)
	: impl(INFO, 0, file, line) {

}

Logger::Logger(SourceFile file, int32_t line, LogLevel level, const char *func)
	: impl(level, 0, file, line) {
	impl.stream << func << ' ';
}

Logger::Logger(SourceFile file, int32_t line, LogLevel level)
	: impl(level, 0, file, line) {

}

Logger::~Logger() {
	impl.finish();
	const LogStream::Buffer &buf(stream().getBuffer());
	g_output(buf.getData(), buf.length());
	if (impl.level == FATAL) {
		g_flush();
		abort();
	}
}

void Logger::setLogLevel(Logger::LogLevel level) {
	g_logLevel = level;
}

void Logger::setOutput(const OutputFunc &func) {
	g_output = func;
}

void Logger::setFlush(const FlushFunc &func) {
	g_flush = func;
}
