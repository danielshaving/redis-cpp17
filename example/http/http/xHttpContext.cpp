#include "xHttpContext.h"
#include "xBuffer.h"

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

 bool xHttpContext::wsFrameExtractBuffer(const char *buffer,const size_t bufferSize,
		 xHttpRequest::WebSocketType &outopcode,size_t &frameSize,bool &outfin)
{
	if(bufferSize < 2)
	{
		return false;
	}

	outfin = (buffer[0] & 0x80) != 0;
	outopcode = (xHttpRequest::WebSocketType)(buffer[0] & 0x0F);

	const bool isMasking = (buffer[1] & 0x80) != 0;
	uint32_t payloadlen = buffer[1] & 0x7F;

	uint32_t pos = 2;
	if (payloadlen == 126)
	{
		if (bufferSize < 4)
		{
			return false;
		}

		payloadlen = (buffer[2] << 8) + buffer[3];
		pos = 4;
	}
	else if(payloadlen == 127)
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

		payloadlen = (buffer[6] << 24) + (buffer[7] << 16) + (buffer[8] << 8) + buffer[9];
		pos = 10;
	}

	uint8_t mask[4];
	if (isMasking)
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

	if (bufferSize < (pos + payloadlen))
	{
		return false;
	}

	if (isMasking)
	{
		for (size_t i = pos, j = 0; j < payloadlen; i++, j++)
		{
			request.parseString.push_back(buffer[i] ^ mask[j % 4]);
		}
	}
	else
	{
		request.parseString.append((const char*)(buffer + pos), payloadlen);
	}

	frameSize = payloadlen + pos;

	 return true;
}

bool xHttpContext::wsFrameBuild(const char *payload, size_t payloadLen,xBuffer *buffer,
		xHttpRequest::WebSocketType framType,bool isFin,bool masking)
 {
	uint8_t head = (uint8_t)framType | (isFin ? 0x80 : 0x00);
	buffer->appendUInt8(head);
	if (payloadLen <= 125)
	{
		buffer->appendUInt8(payloadLen);
	}
	else if (payloadLen <= 0xFFFF)
	{
		buffer->appendUInt8(126);
		buffer->appendUInt8((payloadLen & 0xFF00) >> 8);
		buffer->appendUInt8(payloadLen & 0x00FF);
	 }
	 else
	 {
		 buffer->appendUInt8(127);
		 buffer->appendUInt8(0x00);
		 buffer->appendUInt8(0x00);
		 buffer->appendUInt8(0x00);
		 buffer->appendUInt8(0x00);

		 buffer->appendUInt8((payloadLen & 0xFF000000) >> 24);
		 buffer->appendUInt8((payloadLen & 0x00FF0000) >> 16);
		 buffer->appendUInt8((payloadLen & 0x0000FF00) >> 8);
		 buffer->appendUInt8(payloadLen & 0x000000FF);
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

		 for (size_t i = 0; i < payloadLen; i++)
		 {
			 buffer->appendUInt8(payload[i] ^ mask[i % 4]);
		 }
	 }
	 else
	 {
		 buffer->append(payload,payloadLen);
	 }

	 return true;
 }

bool xHttpContext::parseWebRequest(const TcpConnectionPtr &conn,xBuffer *buf)
{
	bool ok = true;
	while(buf->readableBytes() > 0)
	{
		request.parseString.clear();
		request.opcode = xHttpRequest::ERROR_FRAME;
		size_t frameSize = 0;
		bool isFin = false;
		if (!wsFrameExtractBuffer(buf->peek(),buf->readableBytes(),request.opcode,frameSize,isFin))
		{
			buf->retrieve(frameSize);
			ok = false;
			break;
		}

		if (isFin && (request.opcode == xHttpRequest::TEXT_FRAME ||
				request.opcode == xHttpRequest::BINARY_FRAME))
		{
			if (!request.cacheFrame.empty())
			{
				request.cacheFrame += request.parseString;
				request.parseString = std::move(request.cacheFrame);
				request.cacheFrame.clear();
				ok = false;
			}
		}
		else if(request.opcode == xHttpRequest::CONTINUATION_FRAME)
		{
			request.cacheFrame += request.parseString;
			request.parseString.clear();
			ok = false;
		}
		else if (request.opcode == xHttpRequest::PING_FRAME ||
				request.opcode == xHttpRequest::PONG_FRAME )
		{
			LOG_INFO<<"request.opcode "<<request.opcode;
		}
		else if(request.opcode == xHttpRequest::CLOSE_FRAME)
		{
			LOG_INFO<<"CLOSE_FRAME:"<<request.cacheFrame;
			LOG_INFO<<"CLOSE_FRAME:"<<request.parseString;
			conn->shutdown();
		}
		else
		{
			assert(false);
		}

		buf->retrieve(frameSize);
	}

	return ok;
}

bool xHttpContext::parseRequest(xBuffer *buf)
{
	bool ok = true;
	bool hasMore = true;
	while(hasMore)
	{
		if(state == kExpectRequestLine)
		{
			const char * crlf = buf->findCRLF();
			if(crlf)
			{
				ok = processRequestLine(buf->peek(),crlf);
				if(ok)
				{
					request.setReceiveTime(time(0));
					buf->retrieveUntil(crlf + 2);
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
			const char *crlf = buf->findCRLF();
			if(crlf)
			{
				const char *colon = std::find(buf->peek(), crlf, ':');
				if(colon != crlf)
				{
					request.addHeader(buf->peek(),colon,crlf);
				}
				else
				{
					state = kGotAll;
					hasMore = false;
				}
				buf->retrieveUntil(crlf + 2);
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




























