#include "xHttpResponse.h"

void xHttpResponse::swap(xBuffer &buf)
{
	sendBuf.swap(buf);
}
