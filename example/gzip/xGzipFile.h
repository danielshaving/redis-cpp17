#pragma once
#include "xAll.h"
#include <zlib.h>

class xGzipFile : boost::noncopyable
{
public:
	xGzipFile(xGzipFile && rhs)
	:file(rhs.file)
	{
		rhs.file = nullptr;
	}

	~xGzipFile()
	{
		if(file)
		{
			::gzclose(file);
		}
	}

	xGzipFile &operator = (xGzipFile && rhs)
	{
		swap(rhs);
		return *this;
	}

	bool valid() const { return file != nullptr; }
	void swap(xGzipFile &rhs) { std::swap(file,rhs.file); }
#if ZLIB_VERNUM >= 0x1240
	bool setBuffer(int32_t size) { return ::gzbuffer(file,size) == 0; }
#endif


	// return the number of uncompressed bytes actually read, 0 for eof, -1 for error
	int32_t read(void *buf,int32_t len) { return ::gzread(file,buf,len); }

	// return the number of uncompressed bytes actually written
	int32_t write(xStringPiece buf) { return ::gzwrite(file,buf.data(),buf.size()); }

	// number of uncompressed bytes
	off_t tell() const { return ::gztell(file); }

#if ZLIB_VERNUM >= 0x1240
	// number of compressed bytes
	off_t offset() const { return ::gzoffset(file); }
#endif

	// int flush(int f) { return ::gzflush(file_, f); }

	static xGzipFile openForRead(xStringArg filename)
	{
		return xGzipFile(::gzopen(filename.c_str(),"rbe"));
	}

	static xGzipFile openForAppend(xStringArg filename)
	{
		return xGzipFile(::gzopen(filename.c_str(),"abe"));
	}

	static xGzipFile openForWriteExclusive(xStringArg filename)
	{
		return xGzipFile(::gzopen(filename.c_str(),"wbxe"));
	}

	static xGzipFile openForWriteTruncate(xStringArg filename)
	{
		return xGzipFile(::gzopen(filename.c_str(),"wbe"));
	}



private:
	explicit xGzipFile(gzFile file)
    :file(file)
  	{

  	}

  gzFile file;
};

