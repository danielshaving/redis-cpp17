#pragma once

#include "all.h"

class xBuffer:noncopyable
{
public:
	static const size_t kCheapPrepend = 8;
	static const size_t kInitialSize = 1024 * 64;

explicit xBuffer(size_t initialSize = kInitialSize)
	: buffer(kCheapPrepend + initialSize),
	readerIndex(kCheapPrepend),
	writerIndex(kCheapPrepend)
	{
		assert(readableBytes() == 0);
		assert(writableBytes() == initialSize);
		assert(prependableBytes() == kCheapPrepend);
	}

	void swap(xBuffer& rhs)
	{
		buffer.swap(rhs.buffer);
		std::swap(readerIndex, rhs.readerIndex);
		std::swap(writerIndex, rhs.writerIndex);
	}

	size_t readableBytes() const { return writerIndex - readerIndex; }
	size_t writableBytes() const { return buffer.size() - writerIndex; }
	size_t prependableBytes() const { return readerIndex; }

	const char* peek() const { return begin() + readerIndex; }

	const char* findCRLF() const
	{
		const char* crlf = std::search(peek(), beginWrite(), kCRLF, kCRLF+2);
		return crlf == beginWrite() ? nullptr : crlf;
	}

	const char* findCRLF(const char* start) const
	{
		assert(peek() <= start);
		assert(start <= beginWrite());
		const char* crlf = std::search(start, beginWrite(), kCRLF, kCRLF+2);
		return crlf == beginWrite() ? nullptr : crlf;
	}

	const char* findEOL() const
	{
		const void* eol = memchr(peek(), '\n', readableBytes());
		return static_cast<const char*>(eol);
	}

	const char* findCONTENT()const
	{
		const char* content = std::search(peek(), beginWrite(), CONTENT, CONTENT+14);
		return content == beginWrite() ? nullptr : content;
	}

	const char* findEOL(const char* start) const
	{
		assert(peek() <= start);
		assert(start <= beginWrite());
		const void* eol = memchr(start, '\n', beginWrite() - start);
		return static_cast<const char*>(eol);
	}

	void retrieve(size_t len)
	{
		assert(len <= readableBytes());
		if (len < readableBytes())
		{
			readerIndex += len;
		}
		else
		{
			retrieveAll();
		}
	}

	void retrieveUntil(const char* end)
	{
		assert(peek() <= end);
		assert(end <= beginWrite());
		retrieve(end - peek());
	}

	void retrieveInt64()
	{
		retrieve(sizeof(int64_t));
	}

	void retrieveInt32()
	{
		retrieve(sizeof(int32_t));
	}

	void retrieveInt16()
	{
		retrieve(sizeof(int16_t));
	}

	void retrieveInt8()
	{
		retrieve(sizeof(int8_t));
	}

	void retrieveAll()
	{
		readerIndex = kCheapPrepend;
		writerIndex = kCheapPrepend;
	}

	void resize()
	{

	}

	std::string retrieveAllAsString()
	{
		return retrieveAsString(readableBytes());;
	}

	std::string retrieveAsString(size_t len)
	{
		assert(len <= readableBytes());
		std::string result(peek(), len);
		retrieve(len);
		return result;
	}

	void append(const char* /*restrict*/ data, size_t len)
	{
		ensureWritableBytes(len);
		std::copy(data, data+len, beginWrite());
		hasWritten(len);
	}

	void append(const xStringPiece & str)
	{
		append(str.data(), str.size());
	}

	void append(const void* /*restrict*/ data, size_t len)
	{
		append(static_cast<const char*>(data), len);
	}

	void appendInt32(int32_t x)
	{
		int32_t be32 = x;
		append(&be32, sizeof be32);
	}

	void appendInt16(int16_t x)
	{
		int16_t be16 = x;
		append(&be16, sizeof be16);
	}

	void appendInt8(int8_t x)
	{
		append(&x, sizeof x);
	}


	void prependInt64(int64_t x)
	{
		int64_t be64 = x;
		prepend(&be64, sizeof be64);
	}

	void prependInt32(int32_t x)
	{
		int32_t be32 = x;
		prepend(&be32, sizeof be32);
	}

	void prependInt16(int16_t x)
	{
		int16_t be16 = x;
		prepend(&be16, sizeof be16);
	}

	void prependInt8(int8_t x)
	{
		prepend(&x, sizeof x);
	}

	void prepend(const void * data, size_t len)
	{
		assert(len <= prependableBytes());
		readerIndex -= len;
		const char * d = static_cast<const char*>(data);
		std::copy(d,d + len,begin() + readerIndex);
	}

	void preapend(const void * data, size_t len)
	{
		prepend(static_cast<const char*>(data),len);
	}

	void preapend(const char * /*restrict*/ data, size_t len)
	{
		ensureWritableBytes(len);
		std::copy(prepeek(), prepeek() + writableBytes(), prepeek() + len);
		std::copy(data, data + len, prepeek());
		hasWritten(len);
	}

	void ensureWritableBytes(size_t len)
	{
		if (writableBytes() < len)
		{
			makeSpace(len);
		}
		assert(writableBytes() >= len);
	}

	char* beginWrite() { return begin() + writerIndex; }
	const char* beginWrite() const { return begin() + writerIndex; }

	void hasWritten(size_t len)
	{
		assert(len <= writableBytes());
		writerIndex += len;
	}

	void unwrite(size_t len)
	{
		assert(len <= readableBytes());
		writerIndex -= len;
	}

	int64_t readInt64()
	{
		int64_t result = peekInt64();
		retrieveInt64();
		return result;
	}

	int32_t readInt32()
	{
		int32_t result = peekInt32();
		retrieveInt32();
		return result;
	}

	int16_t readInt16()
	{
		int16_t result = peekInt16();
		retrieveInt16();
		return result;
	}

	int8_t readInt8()
	{
		int8_t result = peekInt8();
		retrieveInt8();
		return result;
	}

	int64_t peekInt64() const
	{
		assert(readableBytes() >= sizeof(int64_t));
		int64_t be64 = 0;
		::memcpy(&be64, peek(), sizeof be64);
		return be64;
	}

	int32_t peekInt32() const
	{
		assert(readableBytes() >= sizeof(int32_t));
		int32_t be32 = 0;
		::memcpy(&be32, peek(), sizeof be32);
		return be32;
	}

	int16_t peekInt16() const
	{
		assert(readableBytes() >= sizeof(int16_t));
		int16_t be16 = 0;
		::memcpy(&be16, peek(), sizeof be16);
		return be16;
	}

	int8_t peekInt8() const
	{
		assert(readableBytes() >= sizeof(int8_t));
		int8_t x = *peek();
		return x;
	}

	xStringPiece toStringPiece() const
	{
		return xStringPiece(peek(), static_cast<int>(readableBytes()));
	}

	void shrink(size_t reserve)
	{
		xBuffer other;
		other.ensureWritableBytes(readableBytes()+reserve);
		other.append(toStringPiece());
		swap(other);
	}


	size_t internalCapacity() const
	{
		return buffer.capacity();
	}
	ssize_t readFd(int fd, int* savedErrno);

private:
	char* begin() { return &*buffer.begin(); }
	char *prepeek() { return begin() + readerIndex; }
	const char* begin() const { return &*buffer.begin(); }

	void makeSpace(size_t len)
	{
		if (writableBytes() + prependableBytes() < len + kCheapPrepend)
		{
			buffer.resize(writerIndex+len);
		}
		else
		{
			assert(kCheapPrepend < readerIndex);
			size_t readable = readableBytes();
			std::copy(begin()+readerIndex,
				begin()+writerIndex,
				begin()+kCheapPrepend);
			readerIndex = kCheapPrepend;
			writerIndex = readerIndex + readable;
			assert(readable == readableBytes());
		}
	}
private:
	std::vector<char> buffer;
	size_t readerIndex;
	size_t writerIndex;

	static const char kCRLF[];
	static const char kCRLFCRLF[];
	static const char CONTENT[];
};




