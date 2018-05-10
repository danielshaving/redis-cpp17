#include "httpcontext.h"

bool HttpContext::processRequestLine(const char *begin,const char *end)
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

bool HttpContext::wsFrameExtractBuffer(const TcpConnectionPtr &conn,const char *buf,const size_t bufferSize,size_t &size,bool &fin)
{
	if(bufferSize < 2)
	{
		return false;
	}

	const unsigned char *buffer = (const unsigned char*)buf;
	fin = (buffer[0] >> 7) & 0x01;
	const bool masking = (buffer[1] & 0x80) == 0x80;
	uint32_t len = buffer[1] & 0x7F;
	uint32_t pos = 2;

	if(len <= 125)
	{

	}
	else if (len == 126)
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

		if ((buffer[6] & 0x80) == 0x80)
		{
			return false;
		}

		len = (buffer[6] << 24) + (buffer[7] << 16) + (buffer[8] << 8) + (buffer[9] << 0);
		pos = 10;
	}
	else
	{
		conn->forceClose();
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
		auto &parseString = getRequest().getParseString();
		for (size_t i = pos, j = 0; j < len; i++, j++)
		{
			parseString.push_back(buffer[i] ^ mask[j % 4]);
		}
	}
	else
	{
		conn->forceClose();
	}

	if(fin)
	{
		getRequest().setOpCodeType((xHttpRequest::WebSocketType)(buffer[0] & 0x0F));
	}
	else
	{
		getRequest().setOpCodeType(xHttpRequest::CONTINUATION_FRAME);
	}

	size = len + pos;
	return true;
}

bool HttpContext::wsFrameBuild(Buffer *buffer,xHttpRequest::WebSocketType framType,bool ok,bool masking)
 {
	 if (masking)
	 {
		 char *peek = buffer->start();
		 peek[1] = ((uint8_t)peek[1]) | 0x80;
		 uint8_t mask[4];

		 size_t size = sizeof(mask) / sizeof(mask[0]) - 1;

		 for (size_t i = size ; i >=0; i--)
		 {
			 mask[i] = rand();
			 buffer->prependInt8(mask[i]);
		 }

		 for (size_t i = 0; i < buffer->readableBytes(); i++)
		 {
			 peek[i] = peek[i] ^ mask[i % 4];
		 }
	 }

	if (buffer->readableBytes() <= 125)
	{
		buffer->prependInt8(buffer->readableBytes());
	}
	else if (buffer->readableBytes() <= 0xFFFF)
	{
		buffer->prependInt8(buffer->readableBytes() & 0x00FF);
		buffer->prependInt8((buffer->readableBytes() & 0xFF00) >> 8);
		buffer->prependInt8(126);
	 }
	 else
	 {
		 buffer->prependInt8((buffer->readableBytes() & 0x000000FF) >> 0);
		 buffer->prependInt8((buffer->readableBytes() & 0x0000FF00) >> 8);
		 buffer->prependInt8((buffer->readableBytes() & 0x00FF0000) >> 16);
		 buffer->prependInt8((buffer->readableBytes() & 0xFF000000) >> 24);
		 buffer->prependInt8(0x00);
		 buffer->prependInt8(0x00);
		 buffer->prependInt8(0x00);
		 buffer->prependInt8(0x00);
		 buffer->prependInt8(127);
	 }

	uint8_t head = (uint8_t)framType | (ok ? 0x80 : 0x00);
	buffer->prependInt8(head);

	return true;
 }

bool HttpContext::parseRequest(Buffer *buffer)
{
	bool ok = true;
	bool hasMore = true;
	while(hasMore)
	{
		if(state == kExpectRequestLine)
		{
			const char *crlf = buffer->findCRLF();
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





























