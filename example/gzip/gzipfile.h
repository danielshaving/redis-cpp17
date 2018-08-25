#pragma once
#include "all.h"
#include <zlib.h>

class GzipFile : boost::noncopyable
{
public:
	GzipFile(GzipFile && rhs)
		:file(rhs.file)
	{
		rhs.file = nullptr;
	}

	~GzipFile()
	{
		if (file)
		{
			::gzclose(file);
		}
	}

	GzipFile &operator = (GzipFile && rhs)
	{
		swap(rhs);
		return *this;
	}

	bool valid() const { return file != nullptr; }
	void swap(GzipFile &rhs) { std::swap(file, rhs.file); }
#if ZLIB_VERNUM >= 0x1240
	bool setBuffer(int32_t size) { return ::gzbuffer(file, size) == 0; }
#endif

	// return the number of uncompressed bytes actually read, 0 for eof, -1 for error
	int32_t read(void *buf, int32_t len) { return ::gzread(file, buf, len); }

	// return the number of uncompressed bytes actually written
	int32_t write(StringPiece buf) { return ::gzwrite(file, buf.data(), buf.size()); }

	// number of uncompressed bytes
	off_t tell() const { return ::gztell(file); }

#if ZLIB_VERNUM >= 0x1240
	// number of compressed bytes
	off_t offset() const { return ::gzoffset(file); }
#endif

	// int flush(int f) { return ::gzflush(file_, f); }

	static GzipFile openForRead(xStringArg filename)
	{
		return GzipFile(::gzopen(filename.c_str(), "rbe"));
	}

	static GzipFile openForAppend(xStringArg filename)
	{
		return GzipFile(::gzopen(filename.c_str(), "abe"));
	}

	static GzipFile openForWriteExclusive(xStringArg filename)
	{
		return GzipFile(::gzopen(filename.c_str(), "wbxe"));
	}

	static GzipFile openForWriteTruncate(xStringArg filename)
	{
		return GzipFile(::gzopen(filename.c_str(), "wbe"));
	}

private:
	explicit GzipFile(gzFile file)
		:file(file)
	{

	}
	gzFile file;
};

