#include "xHttpContext.h"

bool xHttpContext::processRequestLine(const char *begin,const char *end)
{
	bool succeed = false;
	const char *start = begin;
	const  char *space = std::find(start,end,' ');
	if(space != end && request.setMethod(start,space))
	{
		start = space+1;
		space = std::find(start, end, ' ');
		if (space != end)
		{
			const char *question = std::find(start,space ,'?');
			if(question != space)
			{
				request.setPath(start,question);
				request.setQuery(question + 1,space);
			}
			else
			{
				request.setPath(start,space);
			}

			start = space + 1;
			succeed = end - start == 8 && std::equal(start,end - 1,"HTTP/1.");
			if(succeed)
			{
				if (*(end-1) == '1')
				{
					request.setVersion(xHttpRequest::kHttp11);
				}
				else if (*(end-1) == '0')
				{
					request.setVersion(xHttpRequest::kHttp10);
				}
				else
				{
					succeed = false;
				}
			}
		}
	}
	return succeed;
}

bool xHttpContext::wsFrameExtractBuffer(const char *buf,const size_t bufferSize,size_t &size,bool &ok)
{
	const unsigned char* buffer = (const unsigned char*)buf;

	if(bufferSize < 2)
	{
		return false;
	}

	ok = (buffer[0] & 0x80) != 0;
	request.setOpCodeType((xHttpRequest::WebSocketType)(buffer[0] & 0x0F));

	const bool masking = (buffer[1] & 0x80) != 0;
	uint32_t len = buffer[1] & 0x7F;

	uint32_t pos = 2;
	if (len == 126)
	{
		if (bufferSize < 4)
		{
			return false;
		}

		len = (buffer[2] << 8) + buffer[3];
		pos = 4;
	}
	else if(len == 127)
	{
		if (bufferSize < 10)
		{
			return false;
		}

		if (buffer[2] != 0 ||
		buffer[3] != 0 ||
		buffer[4] != 0 ||
		buffer[5] != 0)
		{
			return false;
		}

		if ((buffer[6] & 0x80) != 0)
		{
			return false;
		}

		len = (buffer[6] << 24) + (buffer[7] << 16) + (buffer[8] << 8) + buffer[9];
		pos = 10;
	}

	uint8_t mask[4];
	if (masking)
	{
		if (bufferSize < (pos + 4))
		{
			return false;
		}

		mask[0] = buffer[pos++];
		mask[1] = buffer[pos++];
		mask[2] = buffer[pos++];
		mask[3] = buffer[pos++];
	}

	if (bufferSize < (pos + len))
	{
		return false;
	}

	if (masking)
	{
		for (size_t i = pos, j = 0; j < len; i++, j++)
		{
			request.outputBuffer().appendUInt8(buffer[i] ^ mask[j % 4]);
		}
	}
	else
	{
		request.outputBuffer().append((buffer + pos), len);
	}

	size = len + pos;

	 return true;
}

bool xHttpContext::wsFrameBuild(const char *payload, size_t size,xBuffer *buffer,
		xHttpRequest::WebSocketType framType,bool ok,bool masking)
 {
	uint8_t head = (uint8_t)framType | (ok ? 0x80 : 0x00);
	buffer->appendUInt8(head);
	if (size <= 125)
	{
		buffer->appendUInt8(size);
	}
	else if (size <= 0xFFFF)
	{
		buffer->appendUInt8(126);
		buffer->appendUInt8((size & 0xFF00) >> 8);
		buffer->appendUInt8(size & 0x00FF);
	 }
	 else
	 {
		 buffer->appendUInt8(127);
		 buffer->appendUInt8(0x00);
		 buffer->appendUInt8(0x00);
		 buffer->appendUInt8(0x00);
		 buffer->appendUInt8(0x00);

		 buffer->appendUInt8((size & 0xFF000000) >> 24);
		 buffer->appendUInt8((size & 0x00FF0000) >> 16);
		 buffer->appendUInt8((size & 0x0000FF00) >> 8);
		 buffer->appendUInt8(size & 0x000000FF);
	 }

	 if (masking)
	 {
		 char *peek = buffer->cpeek();
		 peek[1] = ((uint8_t)peek[1]) | 0x80;
		 uint8_t mask[4];
		 for (size_t i = 0; i < sizeof(mask) / sizeof(mask[0]); i++)
		 {
			 mask[i] = rand();
			 buffer->appendUInt8(mask[i]);
		 }

		 for (size_t i = 0; i < size; i++)
		 {
			 buffer->appendUInt8(payload[i] ^ mask[i % 4]);
		 }
	 }
	 else
	 {
		 buffer->append(payload,size);
	 }

	 return true;
 }

bool xHttpContext::parseRequest(xBuffer *buffer)
{
	bool ok = true;
	bool hasMore = true;
	while(hasMore)
	{
		if(state == kExpectRequestLine)
		{
			const char * crlf = buffer->findCRLF();
			if(crlf)
			{
				ok = processRequestLine(buffer->peek(),crlf);
				if(ok)
				{
					buffer->retrieveUntil(crlf + 2);
					state = kExpectHeaders;
				}
				else
				{
					hasMore = false;
				}
			}
			else
			{
				hasMore = false;
			}
		}
		else if(state == kExpectHeaders)
		{
			const char *crlf = buffer->findCRLF();
			if(crlf)
			{
				const char *colon = std::find(buffer->peek(), crlf, ':');
				if(colon != crlf)
				{
					request.addHeader(buffer->peek(),colon,crlf);
				}
				else
				{
					state = kGotAll;
					hasMore = false;
				}
				buffer->retrieveUntil(crlf + 2);
			}
			else
			{
				hasMore = false;
			}
		}
		else if(state == kExpectBody)
		{

		}
	}
	return ok;
}




























