#include "xLog.h"

const char digits[] = "9876543210123456789";
const char digitsHex[] = "0123456789ABCDEF";
const char* zero = digits + 9;
template<typename T>
size_t convert(char buf[], T value)
{
	T i = value;
	char* p = buf;

	do
	{
		int32_t lsd = static_cast<int32_t>(i % 10);
		i /= 10;
		*p++ = zero[lsd];
	} while (i != 0);

	if (value < 0)
	{
		*p++ = '-';
	}

	*p = '\0';
	std::reverse(buf,p);

	return p - buf;
}


size_t convertHex(char buf[],uintptr_t value)
{
	uintptr_t i = value;
	char *p = buf;

	do
	{
		int32_t lsd = static_cast<int32_t>(i % 16);
		i /= 16;
		*p++ = digitsHex[lsd];
	} while (i != 0);

	*p = '\0';
	std::reverse(buf,p);

	return p - buf;
}

xAppendFile::xAppendFile(std::string &filename)
  : fp(::fopen(filename.c_str(),"ae")),  // 'e' for O_CLOEXEC
    writtenBytes(0)
{
	assert(fp);
	::setbuffer(fp,buffer,sizeof(buffer));
}

xAppendFile::~xAppendFile()
{
	::fclose(fp);
}

void xAppendFile::append(const char *logline,const size_t len)
{
	size_t n = write(logline, len);
	size_t remain = len - n;
	while (remain > 0)
	{
		size_t x = write(logline + n, remain);
		if (x == 0)
		{
			int32_t err = ferror(fp);
			if (err)
			{
				LOG_ERROR<<"xAppendFile::append() failed "<< strerror(err);
			}
			break;
		}
		n += x;
		remain = len - n; // remain -= x
	}

	writtenBytes += len;
}

void xAppendFile::flush()
{
	::fflush(fp);
}

size_t xAppendFile::write(const char* logline,size_t len)
{
#ifdef __linux__
  return ::fwrite_unlocked(logline,1,len,fp);
#endif

#ifdef __APPLE__
  return ::fwrite(logline,1,len,fp);
#endif
}

xLogFile::xLogFile(const std::string &basename,
                 size_t rollSize,
                 bool threadSafe,
                 int32_t flushint32_terval,
                 int32_t checkEveryN)
  : basename(basename),
    rollSize(rollSize),
    flushint32_terval(flushint32_terval),
    checkEveryN(checkEveryN),
    count(0),
    startOfPeriod(0),
    lastRoll(0),
    lastFlush(0)
{
	assert(basename.find('/') == std::string::npos);
	rollFile();
}

xLogFile::~xLogFile()
{

}

void xLogFile::append(const char *logline, int32_t len)
{
	std::unique_lock<std::mutex> lk(mutex);
	append_unlocked(logline, len);
}

void xLogFile::flush()
{
	std::unique_lock<std::mutex> lk(mutex);
	file->flush();
}

void xLogFile::append_unlocked(const char *logline, int32_t len)
{

  file->append(logline, len);
  if (file->getWrittenBytes() > rollSize)
  {
	  rollFile();
  }
  else
  {
    ++count;
    if (count >= checkEveryN)
    {
		count = 0;
		time_t now = ::time(NULL);
		time_t thisPeriod = now / kRollPerSeconds * kRollPerSeconds;
		if (thisPeriod != startOfPeriod)
		{
			rollFile();
		}
		else if (now - lastFlush > flushint32_terval)
		{
			lastFlush = now;
			file->flush();
		}
    }
  }
}

bool xLogFile::rollFile()
{
	time_t now = 0;
	std::string filename = getLogFileName(basename, &now);
	time_t start = now / kRollPerSeconds* kRollPerSeconds;

	if (now > lastRoll)
	{
		lastRoll = now;
		lastFlush = now;
		startOfPeriod = start;
		file.reset(new xAppendFile(filename));
		return true;
	}
	return false;
}

std::string xLogFile::getLogFileName(const std::string &basename, time_t *now)
{
	std::string filename;
	filename.reserve(basename.size() + 64);
	filename = basename;

	char timebuf[32];
	struct tm tm;
	*now = time(NULL);
	gmtime_r(now, &tm); // FIXME: localtime_r ?
	strftime(timebuf, sizeof timebuf, ".%Y%m%d-%H%M%S", &tm);
	filename += timebuf;
	filename += ".log";
	return filename;
}

xAsyncLogging::xAsyncLogging(std::string baseName,size_t rollSize,int32_t flushint32_terval)
:baseName(baseName),
flushint32_terval(flushint32_terval),
running(false),
rollSize(rollSize),
thread(nullptr),
currentBuffer(new Buffer),
nextBuffer(new Buffer),
buffers()
{
	currentBuffer->bzero();
	nextBuffer->bzero();
	buffers.reserve(16);
}

void xAsyncLogging::append(const char *logline,int32_t len)
{
	std::unique_lock<std::mutex> lk(mutex);
	if (currentBuffer->avail() > len)
	{
		currentBuffer->append(logline,len);
	}
	else
	{
		buffers.push_back(std::unique_ptr<Buffer>(currentBuffer.release()));
		if (nextBuffer)
		{
			currentBuffer = std::move(nextBuffer);
		}
		else
		{
			currentBuffer.reset(new Buffer);
		}

		currentBuffer->append(logline,len);
		condition.notify_one();
	}
}

void xAsyncLogging::threadFunc()
{
	xLogFile output(baseName,rollSize,false);
	BufferPtr newBuffer1(new Buffer);
	BufferPtr newBuffer2(new Buffer);
	newBuffer1->bzero();
	newBuffer2->bzero();

	BufferVector buffersToWrite;
	buffersToWrite.reserve(16);

	running = true;

	while(running)
	{
		assert(newBuffer1 && newBuffer1->length() == 0);
		assert(newBuffer2 && newBuffer2->length() == 0);
		assert(buffersToWrite.empty());
		{
			std::unique_lock<std::mutex> lk(mutex);
			if (buffers.empty())
			{
				condition.wait_for(lk,std::chrono::seconds(flushint32_terval));
			}

			buffers.push_back(std::unique_ptr<Buffer>(currentBuffer.release()));
			currentBuffer = std::move(newBuffer1);
			buffersToWrite.swap(buffers);

			if (!nextBuffer)
			{
				nextBuffer = std::move(newBuffer2);
			}
		}

		assert(!buffersToWrite.empty());

		if (buffersToWrite.size() > 25)
		{

		}

		for (size_t i = 0; i < buffersToWrite.size(); ++i)
		{
			output.append(buffersToWrite[i]->getData(), buffersToWrite[i]->length());
		}

		if (buffersToWrite.size() > 2)
		{
			buffersToWrite.resize(2);
		}

		if (!newBuffer1)
		{
			assert(!buffersToWrite.empty());
			newBuffer1 = std::move(buffersToWrite.back());
			buffersToWrite.pop_back();
			newBuffer1->reset();
		}

		if (!newBuffer2)
		{
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
void xLogStream::formatInteger(T v)
{
	if (buffer.avail() >= kMaxNumericSize)
	{
		size_t len = convert(buffer.current(), v);
		buffer.add(len);
	}
}

xLogStream &xLogStream::operator<<(short v)
{
	*this << static_cast<int>(v);
	return *this;
}

xLogStream &xLogStream::operator<<(unsigned short v)
{
	*this << static_cast<unsigned int>(v);
	return *this;
}

xLogStream &xLogStream::operator<<(int v)
{
	formatInteger(v);
	return *this;
}

xLogStream &xLogStream::operator<<(unsigned int v)
{
	formatInteger(v);
	return *this;
}

xLogStream &xLogStream::operator<<(long v)
{
	formatInteger(v);
	return *this;
}

xLogStream &xLogStream::operator<<(unsigned long v)
{
	formatInteger(v);
	return *this;
}

xLogStream &xLogStream::operator<<(long long v)
{
	formatInteger(v);
	return *this;
}

xLogStream &xLogStream::operator<<(unsigned long long v)
{
	formatInteger(v);
	return *this;
}

xLogStream &xLogStream::operator<<(const void* p)
{
	uintptr_t v = reinterpret_cast<uintptr_t>(p);
	if (buffer.avail() >= kMaxNumericSize)
	{
		char* buf = buffer.current();
		buf[0] = '0';
		buf[1] = 'x';
		size_t len = convertHex(buf+2, v);
		buffer.add(len+2);
	}
	return *this;
}

// FIXME: replace this with Grisu3 by Florian Loitsch.
xLogStream &xLogStream::operator<<(double v)
{
	if (buffer.avail() >= kMaxNumericSize)
	{
		int32_t len = snprintf(buffer.current(), kMaxNumericSize, "%.12g", v);
		buffer.add(len);
	}
	return *this;
}


xLogger::LogLevel initLogLevel()
{
	if (::getenv("TRACE"))
		return xLogger::TRACE;
	else if (::getenv("DEBUG"))
		return xLogger::DEBUG;
	else
		return xLogger::INFO;
}

xLogger::LogLevel g_logLevel = initLogLevel();

const char *LogLevelName[xLogger::NUM_LOG_LEVELS] =
{
  "TRACE ",
  "DEBUG ",
  "INFO  ",
  "WARN  ",
  "ERROR ",
  "FATAL ",
};


void defaultOutput(const char* msg,int32_t len)
{
	size_t n = fwrite(msg,1,len,stdout);
	//FIXME check n
	(void)n;
	printf("%s\n",msg);
}

void defaultFlush()
{
	fflush(stdout);
}

xLogger::OutputFunc g_output = defaultOutput;
xLogger::FlushFunc g_flush = defaultFlush;

xLogger::xImpl::xImpl(LogLevel level,int32_t savedErrno,const xSourceFile &file,int32_t line)
  : stream(),
    level(level),
    line(line),
    baseName(file)
{
	formatTime();
}

void xLogger::xImpl::formatTime()
{
	char t_time[32];
	char timebuf[32];
	struct tm tm_time;
	time_t now = time(0);
	gmtime_r(&now, &tm_time);
	int32_t len = snprintf(t_time,sizeof(t_time),"%4d%02d%02d %02d:%02d:%02d",
	tm_time.tm_year + 1900,tm_time.tm_mon + 1,tm_time.tm_mday,
	tm_time.tm_hour + 8,tm_time.tm_min,tm_time.tm_sec);
	assert(len == 17); (void)len;
	stream<<T(t_time,17);
	stream<<"  ";

}


void xLogger::xImpl::finish()
{
	stream << "  " << T(baseName.data,baseName.size)<< ':' << line<<"\r";
}

xLogger::xLogger(xSourceFile file,int32_t line)
 :impl(INFO, 0, file, line)
{

}

xLogger::xLogger(xSourceFile file,int32_t line,LogLevel level,const char *func)
  : impl(level, 0, file, line)
{
  impl.stream << func << ' ';
}

xLogger::xLogger(xSourceFile file,int32_t line,LogLevel level)
  : impl(level, 0, file, line)
{

}

xLogger::xLogger(xSourceFile file,int32_t line,bool toAbort)
  : impl(toAbort?FATAL:ERROR, errno, file, line)
{

}


xLogger::~xLogger()
{
	impl.finish();
	const xLogStream::Buffer &buf(stream().getBuffer());
	g_output(buf.getData(),buf.length());
	if (impl.level == FATAL)
	{
		g_flush();
		abort();
	}
}

void xLogger::setLogLevel(xLogger::LogLevel level)
{
	g_logLevel = level;
}

void xLogger::setOutput(OutputFunc out)
{
	g_output = out;
}

void xLogger::setFlush(FlushFunc flush)
{
	g_flush = flush;
}
