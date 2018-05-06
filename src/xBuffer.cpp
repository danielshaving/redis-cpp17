#include "xBuffer.h"

const char xBuffer::kCRLF[] = "\r\n";
const char xBuffer::CONTENT[] = "Content-Length";
const size_t xBuffer::kCheapPrepend;
const size_t xBuffer::kInitialSize;

ssize_t xBuffer::readFd(int32_t fd,int32_t *savedErrno)
{
	char extrabuf[65536];
	struct iovec vec[2];
	const size_t writable = writableBytes();
	vec[0].iov_base = begin() + writerIndex;
	vec[0].iov_len = writable;
	vec[1].iov_base = extrabuf;
	vec[1].iov_len = sizeof extrabuf;
	const int32_t iovcnt = (writable < sizeof extrabuf) ? 2 : 1;
	const ssize_t n = ::readv(fd,vec,iovcnt);
	if (n < 0)
	{
		*savedErrno = errno;
	}
	else if (size_t(n) <= writable)
	{
		writerIndex += n;
	}
	else
	{
		writerIndex = buffer.size();
		append(extrabuf,n - writable);
	}

	return n;
}

