#pragma once
#include "xAll.h"
#include "xBuffer.h"

const int32_t kSmallBuffer = 4000;
const int32_t kLargeBuffer = 4000*10;

template<int32_t SIZE>
class xFixedBuffer : boost::noncopyable
{
public:
	xFixedBuffer()
	:cur(data)
	{

	}

	~xFixedBuffer()
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

	void reset(){ cur = data;}
	void bzero(){ ::bzero(data,sizeof data); }

	std::string toString() const { return std::string(data,length()); }
	int32_t avail() const { return static_cast<int32_t>(end() - cur); }
	const char *end() const { return data + sizeof data; }
	xStringPiece toStringPiece() const { return xStringPiece(data, length()); }

private:
	char data[SIZE];
	char *cur;
};

class xAppendFile : boost::noncopyable
{
public:
	explicit xAppendFile(std::string &filename);
	~xAppendFile();
	void append(const char *logline,const size_t len);
	void flush();
	void rename(const std::string &oldname,const std::string &rename);
	size_t getWrittenBytes() const { return writtenBytes; }

private:
	size_t write(const char *logline,size_t len);
	FILE *fp;
	char buffer[64*1024];
	size_t writtenBytes;
};


class xLogFile : boost::noncopyable
{
 public:
	xLogFile(const std::string &basename,
	  size_t rollSize,
	  bool threadSafe = true,
	  int32_t interval = 3,
	  int32_t checkEveryN = 1024);
	~xLogFile();

	void append(const char *logline,int32_t len);
	void flush();
	bool rollFile();

private:
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
	std::unique_ptr<xAppendFile> file;
	const static int32_t kRollPerSeconds = 60*60*24;
};

class xAsyncLogging : boost::noncopyable
{
public:
	xAsyncLogging(std::string baseName,size_t rollSize,int32_t interval = 3);
	~xAsyncLogging()
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
		std::thread t(std::bind(&xAsyncLogging::threadFunc,this));
		t.detach();
	}
	void append(const char *loline,int32_t len);

private:
	void threadFunc();

	typedef xFixedBuffer<kLargeBuffer> Buffer;
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

class xLogStream : boost::noncopyable
{
public:
	typedef xLogStream self;
	typedef xFixedBuffer<kSmallBuffer>  Buffer;

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
		buffer.append(&v, 1);
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

	self &operator<<(const xStringPiece &v)
	{
		buffer.append(v.data(),v.size());
		return *this;
	}


	self &operator<<(const xBuffer &v)
	{
		*this << v.toStringPiece();
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

class xLogger
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

	class xSourceFile
	{
	public:
		template<int32_t N>
		inline xSourceFile(const char (&arr)[N])
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

		explicit xSourceFile(const char *fileName)
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

	xLogger(xSourceFile file,int32_t line);
	xLogger(xSourceFile file,int32_t line,LogLevel level);
	xLogger(xSourceFile file,int32_t line,LogLevel level,const char *func);
	xLogger(xSourceFile file,int32_t line,bool toAbort);
	~xLogger();

	xLogStream &stream() { return impl.stream; }

	static LogLevel logLevel();
	static void setLogLevel(LogLevel level);

	typedef void (*OutputFunc)(const char *msg,int32_t len);
	typedef void (*FlushFunc)();

	static void setOutput(OutputFunc);
	static void setFlush(FlushFunc);

private:
	class xImpl
	{
	public:
		 typedef xLogger::LogLevel LogLevel;
		 xImpl(LogLevel level,int32_t oldErrno,const xSourceFile &file,int32_t line);
		 void formatTime();
		 void finish();

		 xLogStream stream;
		 LogLevel level;
		 int32_t line;
		 xSourceFile baseName;
	};
	xImpl impl;
};

extern xLogger::LogLevel g_logLevel;
inline xLogger::LogLevel xLogger::logLevel()
{
  return g_logLevel;
}


#define LOG_TRACE if (xLogger::logLevel() <= xLogger::TRACE) \
  xLogger(__FILE__, __LINE__, xLogger::TRACE, __func__).stream()
#define LOG_DEBUG if (xLogger::logLevel() <= xLogger::DEBUG) \
  xLogger(__FILE__, __LINE__, xLogger::DEBUG, __func__).stream()
#define LOG_INFO if (xLogger::logLevel() <= xLogger::INFO) \
  xLogger(__FILE__, __LINE__).stream()
#define LOG_WARN xLogger(__FILE__, __LINE__, xLogger::WARN).stream()
#define LOG_ERROR xLogger(__FILE__, __LINE__, xLogger::ERROR).stream()
#define LOG_FATAL xLogger(__FILE__, __LINE__, xLogger::FATAL).stream()
#define LOG_SYSERR xLogger(__FILE__, __LINE__, false).stream()
#define LOG_SYSFATAL xLogger(__FILE__, __LINE__, true).stream()




