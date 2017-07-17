#pragma once
#include "all.h"
const int kSmallBuffer = 4000;
const int kLargeBuffer = 4000*10;

template<int SIZE>
class xFixedBuffer:noncopyable
{
public:
	xFixedBuffer():cur(data){}
	~xFixedBuffer(){}

	void append(const char *buf,size_t len)
	{
		if(avail() > len)
		{
			memcpy(cur,buf,len);
			cur+=len;
		}
	}

	const char * getData()const { return data; }
	int length() const { return static_cast<int>(cur - data); }
	char *current() { return cur; }
	void add(size_t len) { cur +=len; }

	void reset(){ cur = data;}
	void bzero(){ ::bzero(data,sizeof data); }

	std::string toString() const { return std::string(data, length()); }
	int  avail() const { return static_cast<int>(end() - cur); }
	const char* end() const { return data + sizeof data; }
private:
	char data[SIZE];
	char *cur;
};


class AppendFile : noncopyable
{
 public:
  explicit AppendFile(std::string  &filename);

  ~AppendFile();

  void append(const char* logline, const size_t len);

  void flush();

  size_t getWrittenBytes() const { return writtenBytes; }

 private:

  size_t write(const char* logline, size_t len);

  FILE* fp;
  char buffer[64*1024];
  size_t writtenBytes;
};


class xLogFile :noncopyable
{
 public:
	xLogFile(const std::string& basename,
          size_t rollSize,
          bool threadSafe = true,
          int flushInterval = 3,
          int checkEveryN = 1024);
  ~xLogFile();

  void append(const char* logline, int len);
  void flush();
  bool rollFile();

 private:
  void append_unlocked(const char* logline, int len);

  static std::string getLogFileName(const std::string& basename, time_t* now);

  const std::string basename;
  const size_t rollSize;
  const int flushInterval;
  const int checkEveryN;

  int count;

  mutable std::mutex mutex;
  time_t startOfPeriod;
  time_t lastRoll;
  time_t lastFlush;
  std::unique_ptr<AppendFile> file;

  const static int kRollPerSeconds = 60*60*24;
};



class xAsyncLogging:noncopyable
{
public:
	xAsyncLogging(std::string baseName,size_t rollSize,int flushInterval = 3);
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
		thread->join();
	}

	void start()
	{
		thread = new std::thread(std::bind(&xAsyncLogging::threadFunc,this));
		std::unique_lock<std::mutex> lock(mutex);
		while(!running)
		{
			condition.wait(lock);
		}
	}

	void append(const char * loline,int len);
private:
	xAsyncLogging(const xAsyncLogging&);
	void operator = (const xAsyncLogging &);
	void threadFunc();

	typedef xFixedBuffer<kLargeBuffer> Buffer;
	typedef std::vector<std::unique_ptr<Buffer>> BufferVector;
	//typedef BufferVector::auto_type BufferPtr;
	typedef std::unique_ptr<Buffer> BufferPtr;
	std::string baseName;
	const int flushInterval;
	bool running;
	size_t rollSize;
	std::thread * thread;
	mutable std::mutex mutex;
	std::condition_variable condition;
	BufferPtr currentBuffer;
	BufferPtr nextBuffer;
	BufferVector buffers;
};



class T
{
 public:
	T(const char* str)
	: str(str), len(static_cast<int>(strlen(str))) { }
	T(const std::string& str)
	:str(str.data()), len(static_cast<int>(str.size())) { }
	T(const char* str, unsigned len)
	:str(str),len(len) {}
	const char* str;
	const unsigned len;
};



class xLogStream : noncopyable
{
public:
	typedef xLogStream self;
	typedef xFixedBuffer<kSmallBuffer>  Buffer;

	self& operator<<(bool v)
	{
		buffer.append(v ? "1" : "0", 1);
		return *this;
	}

	self& operator<<(short);
	self& operator<<(unsigned short);
	self& operator<<(int);
	self& operator<<(unsigned int);
	self& operator<<(long);
	self& operator<<(unsigned long);
	self& operator<<(long long);
	self& operator<<(unsigned long long);
	self& operator<<(const void*);
	self& operator<<(float v)
	{
		*this << static_cast<double>(v);
		return *this;
	}
	self& operator<<(double);

	self& operator<<(char v)
	{
		buffer.append(&v, 1);
		return *this;
	}

	self& operator<<(const char* str)
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

	self& operator<<(const unsigned char* str)
	{
		return operator<<(reinterpret_cast<const char*>(str));
	}

	self& operator<<(const std::string& v)
	{
		buffer.append(v.c_str(), v.size());
		return *this;
	}

	self& operator<<(const T & v)
	{
		buffer.append(v.str, v.len);
		return *this;
	}

	void append(const char* data, int len) { buffer.append(data, len); }
	const Buffer& getBuffer() const { return buffer; }
	void resetBuffer() { buffer.reset(); }
public:
	template<typename T>
	void formatInteger(T);

	Buffer buffer;
	static const int kMaxNumericSize = 32;
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
		template<int N>
		inline xSourceFile(const char (&arr)[N])
		:data(arr),
		 size(N-1)
		{
			const  char * slash = strrchr(data,'/');
			if(slash)
			{
				data = slash + 1;
				size -= static_cast<int>(data - arr);
			}
		}

		explicit xSourceFile(const  char * fileName)
		:data(fileName)
		{
			const char * slash = strrchr(fileName,'/');
			if(slash)
			{
				data = slash + 1;
			}
			size = static_cast<int> (strlen(data));
		}
		const char * data;
		int size;
	};

	xLogger(xSourceFile file, int line);
	xLogger(xSourceFile file, int line, LogLevel level);
	xLogger(xSourceFile file, int line, LogLevel level, const char* func);
	xLogger(xSourceFile file, int line, bool toAbort);
	~xLogger();

	xLogStream& stream() { return impl.stream; }

	static LogLevel logLevel();
	static void setLogLevel(LogLevel level);

	typedef void (*OutputFunc)(const char* msg, int len);
	typedef void (*FlushFunc)();

	static void setOutput(OutputFunc);
	static void setFlush(FlushFunc);

private:
	class xImpl
	{
	public:
		 typedef xLogger::LogLevel LogLevel;
		 xImpl(LogLevel level,int oldErrno,const xSourceFile &file, int line);
		 void formatTime();
		 void finish();

		 xLogStream stream;
		 LogLevel level;
		 int line;
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
#define LOG_INFO xLogger(__FILE__, __LINE__).stream()
#define LOG_WARN xLogger(__FILE__, __LINE__, xLogger::WARN).stream()
#define LOG_ERROR xLogger(__FILE__, __LINE__, xLogger::ERROR).stream()
#define LOG_FATAL xLogger(__FILE__, __LINE__, xLogger::FATAL).stream()
#define LOG_SYSERR xLogger(__FILE__, __LINE__, false).stream()
#define LOG_SYSFATAL xLogger(__FILE__, __LINE__, true).stream()



