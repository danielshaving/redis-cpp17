#include "httpresponse.h"
#include "buffer.h"

void HttpResponse::appendToBuffer(Buffer *output) const
{
	char buf[32];
	snprintf(buf, sizeof buf, "HTTP/1.1 %d ", statusCode);
	output->append(buf);
	output->append(statusMessage);
	output->append("\r\n");

	if (closeConnection)
	{
		output->append("Connection: close\r\n");
	}
	else
	{
		snprintf(buf, sizeof buf, "Content-Length: %zd\r\n", body.size());
		output->append(buf);
		output->append("Connection: Keep-Alive\r\n");
	}

	for (auto &it : headers)
	{
		output->append(it.first);
		output->append(": ");
		output->append(it.second);
		output->append("\r\n");
	}

	output->append("\r\n");
	output->append(body);
}
