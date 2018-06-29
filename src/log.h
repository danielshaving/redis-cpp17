//
// Created by zhanghao on 2018/6/17.
//
#pragma once
#include "all.h"
#include "buffer.h"

const int32_t kSmallBuffer = 4000;
const int32_t kLargeBuffer = 4000*10;

template<int32_t SIZE>
class FixedBuffer
{
public:
	FixedBuffer()
	:cur(data)
	{

	}

	~FixedBuffer()
	{

	}

	void append(const char *buf,size_t len)
	{
		if(avail() > len)
		{
			memcpy(cur,buf,len);
			cur += len;
		}
	}

	const char *getData() const { return data; }
	int32_t length() const { return static_cast<int32_t>(cur - data); }
	char *current() { return cur; }
	void add(size_t len) { cur += len; }

	void reset() { cur = data;}
	void bzero() { ::bzero(data,sizeof data); }

	std::string toString() const { return std::string(data,length()); }
	int32_t avail() const { return static_cast<int32_t>(end() - cur); }
	const char *end() const { return data + sizeof data; }
	std::string_view toStringView() const { return std::string_view(data,length()); }

private:
	FixedBuffer(const FixedBuffer&);
	void operator=(const FixedBuffer&);

	char data[SIZE];
	char *cur;
};

class AppendFile
{
public:
	explicit AppendFile(std::string &filename);
	~AppendFile();
	void append(const char *logline,const size_t len);
	void flush();
	void rename(const std::string &oldname,const std::string &rename);
	size_t getWrittenBytes() const { return writtenBytes; }

private:
	AppendFile(const AppendFile&);
	void operator=(const AppendFile&);

	size_t write(const char *logline,size_t len);
	FILE *fp;
	char buffer[64*1024];
	size_t writtenBytes;
};


class LogFile
{
 public:
	LogFile(const std::string &basename,
	  size_t rollSize,
	  bool threadSafe = true,
	  int32_t interval = 3,
	  int32_t checkEveryN = 1024);
	~LogFile();

	void append(const char *logline,int32_t len);
	void flush();
	bool rollFile();

private:
	LogFile(const LogFile&);
	void operator=(const LogFile&);

	void append_unlocked(const char *logline,int32_t len);
	void getLogFileName(const std::string& basename,time_t *now);
	const std::string basename;
	const size_t rollSize;
	const int32_t interval;
	const int32_t checkEveryN;

	std::string filename;
	std::string filerename;
	int32_t count;
	std::mutex mutex;
	time_t startOfPeriod;
	time_t lastRoll;
	time_t lastFlush;
	std::unique_ptr<AppendFile> file;
	const static int32_t kRollPerSeconds = 60*60*24;
};

class AsyncLogging
{
public:
	AsyncLogging(std::string baseName,size_t rollSize,int32_t interval = 3);
	~AsyncLogging()
	{
		if(running)
		{
			stop();
		}
	}

	void stop()
	{
		running = false;
		condition.notify_one();
	}

	void start()
	{
		std::thread t(std::bind(&AsyncLogging::threadFunc,this));
		t.detach();
	}
	void append(const char *loline,int32_t len);

private:
	AsyncLogging(const AsyncLogging&);
	void operator=(const AsyncLogging&);

	void threadFunc();

	typedef FixedBuffer<kLargeBuffer> Buffer;
	typedef std::vector<std::unique_ptr<Buffer>> BufferVector;
	typedef std::unique_ptr<Buffer> BufferPtr;
	std::string baseName;
	const int32_t interval;
	bool running;
	size_t rollSize;
	mutable std::mutex mutex;
	std::condition_variable condition;
	BufferPtr currentBuffer;
	BufferPtr nextBuffer;
	BufferVector buffers;
};

class T
{
 public:
	T(const char *str)
	: str(str),len(static_cast<int32_t>(strlen(str))) { }
	T(const std::string& str)
	:str(str.data()),len(static_cast<int32_t>(str.size())) { }
	T(const char *str,unsigned len)
	:str(str),len(len) {}
	const char *str;
	const unsigned len;
};

class LogStream
{
public:
	typedef LogStream self;
	typedef FixedBuffer<kSmallBuffer> Buffer;

	self &operator<<(bool v)
	{
		buffer.append(v ? "1" : "0", 1);
		return *this;
	}

	self &operator<<(short);
	self &operator<<(unsigned short);
	self &operator<<(int);
	self &operator<<(unsigned int);
	self &operator<<(long);
	self &operator<<(unsigned long);
	self &operator<<(long long);
	self &operator<<(unsigned long long);

	self &operator<<(const void*);
	self &operator<<(float v)
	{
		*this << static_cast<double>(v);
		return *this;
	}
	self &operator<<(double);
	self &operator<<(char v)
	{
		buffer.append(&v,1);
		return *this;
	}

	self&operator<<(const char *str)
	{
		if (str)
		{
			buffer.append(str, strlen(str));
		}
		else
		{
			buffer.append("(null)", 6);
		}
		return *this;
	}

	self &operator<<(const unsigned char* str)
	{
		return operator<<(reinterpret_cast<const char*>(str));
	}

	self &operator<<(const std::string &v)
	{
		buffer.append(v.c_str(),v.size());
		return *this;
	}

	self &operator<<(const std::string_view &v)
	{
		buffer.append(v.data(),v.size());
		return *this;
	}


	self &operator<<(const Buffer &v)
	{
		*this << v.toStringView();
		return *this;
	}

	self &operator<<(const T &v)
	{
		buffer.append(v.str, v.len);
		return *this;
	}

	void append(const char *data,int32_t len) { buffer.append(data,len); }
	const Buffer &getBuffer() const { return buffer; }
	void resetBuffer() { buffer.reset(); }

public:
	template<typename T>
	void formatInteger(T);

	Buffer buffer;
	static const int32_t kMaxNumericSize = 32;
};

class Logger
{
public:
	enum LogLevel
	{
		TRACE,
		DEBUG,
		INFO,
		WARN,
		ERROR,
		FATAL,
		NUM_LOG_LEVELS,
	};

	class SourceFile
	{
	public:
		template<int32_t N>
		inline SourceFile(const char (&arr)[N])
		:data(arr),
		 size(N-1)
		{
			const char *slash = strrchr(data,'/');
			if(slash)
			{
				data = slash + 1;
				size -= static_cast<int32_t>(data - arr);
			}
		}

		explicit SourceFile(const char *fileName)
		:data(fileName)
		{
			const char *slash = strrchr(fileName,'/');
			if(slash)
			{
				data = slash + 1;
			}
			size = static_cast<int32_t>(strlen(data));
		}
		const char * data;
		int32_t size;
	};

	Logger(SourceFile file,int32_t line);
	Logger(SourceFile file,int32_t line,LogLevel level);
	Logger(SourceFile file,int32_t line,LogLevel level,const char *func);
	Logger(SourceFile file,int32_t line,bool toAbort);
	~Logger();

	LogStream &stream() { return impl.stream; }

	static LogLevel logLevel();
	static void setLogLevel(LogLevel level);

	typedef void (*OutputFunc)(const char *msg,int32_t len);
	typedef void (*FlushFunc)();

	static void setOutput(OutputFunc);
	static void setFlush(FlushFunc);

private:
	class Impl
	{
	public:
		 typedef Logger::LogLevel LogLevel;
		 Impl(LogLevel level,int32_t oldErrno,const SourceFile &file,int32_t line);
		 void formatTime();
		 void finish();

		 LogStream stream;
		 LogLevel level;
		 int32_t line;
		 SourceFile baseName;
	};
	Impl impl;
};

extern Logger::LogLevel g_logLevel;
inline Logger::LogLevel Logger::logLevel()
{
	return g_logLevel;
}

#define LOG_TRACE if (Logger::logLevel() <= Logger::TRACE) \
  Logger(__FILE__, __LINE__, Logger::TRACE, __func__).stream()
#define LOG_DEBUG if (Logger::logLevel() <= Logger::DEBUG) \
  Logger(__FILE__, __LINE__, Logger::DEBUG, __func__).stream()
#define LOG_INFO if (Logger::logLevel() <= Logger::INFO) \
  Logger(__FILE__, __LINE__).stream()
#define LOG_WARN Logger(__FILE__, __LINE__, Logger::WARN).stream()
#define LOG_ERROR Logger(__FILE__, __LINE__, Logger::ERROR).stream()
#define LOG_FATAL Logger(__FILE__, __LINE__, Logger::FATAL).stream()
#define LOG_SYSERR Logger(__FILE__, __LINE__, false).stream()
#define LOG_SYSFATAL Logger(__FILE__, __LINE__, true).stream()




