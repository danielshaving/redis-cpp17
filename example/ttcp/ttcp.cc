//
// Created by admin on 2018/6/25.
//

#include "ttcp.h"
#include "zmalloc.h"

void TTcp::transmit(const Options& opt)
{

}

void TTcp::receive(const Options& opt)
{

}

int TTcp::read(int sockfd, const void* buf, int length)
{
	int nread = 0;
	while (nread < length)
	{
		ssize_t nr = ::read(socked, static_cast<const char*>(buf) + nread, length - nread);
		if (nr > 0)
		{
			nread += static_cast<int>(nr);
		}
		else if (nr == 0)
		{
			break;
		}
		else
		{
			perror("read");
			break;
		}
	}
	return nread;
}

int TTcp::write(int sockfd, const void* buf, int length)
{
	int written = 0;
	while (written < length)
	{
		ssize_t nw = ::write(sockfd, static_cast<const char*>(buf) + written, length - written);
		if (nw > 0)
		{
			written += static_cast<int>(nw);
		}
		else if (nw == 0)
		{
			break;  // EOF
		}
		else if (errno != EINTR)
		{
			perror("write");
			break;
		}
	}
	return written;
}

void TTcp::blockTransmit(const Options& opt)
{
	struct sockaddr_in addr = resolveOrDie(opt.host.c_str(), opt.port);
	printf("connecting to %s:%d\n", inet_ntoa(addr.sin_addr), opt.port);

	int sockfd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	assert(sockfd >= 0);
	int ret = ::connect(sockfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
	if (ret)
	{
		perror("connect");
		printf("Unable to connect %s\n", opt.host.c_str());
		::close(sockfd);
		return;
	}

	printf("connected\n");
	TimeStamp start(TimeStamp::now());
	struct SessionMessage sessionMessage = { 0, 0 };
	sessionMessage.number = htonl(opt.number);
	sessionMessage.length = htonl(opt.length);
	if (write(sockfd, &sessionMessage, sizeof(sessionMessage)) != sizeof(sessionMessage))
	{
		perror("write SessionMessage");
		exit(1);
	}

	const int totalLen = static_cast<int>(sizeof(int32_t) + opt.length);
	PayloadMessage *payload = static_cast<PayloadMessage*>(zmalloc(totalLen));
	assert(payload);
	payload->length = htonl(opt.length);
	for (int i = 0; i < opt.length; ++i)
	{
		payload->data[i] = "0123456789ABCDEF"[i % 16];
	}

	double totalMb = 1.0 * opt.length * opt.number / 1024 / 1024;
	printf("%.3f MiB in total\n", totalMb);

	for (int i = 0; i < opt.number; ++i)
	{
		int nw = write(sockfd, payload, totalLen);
		assert(nw == totalLen);

		int ack = 0;
		int nr = read(sockfd, &ack, sizeof(ack));
		assert(nr == sizeof(ack));
		ack = ntohl(ack);
		assert(ack == opt.length);
	}

	zfree(payload);
	::close(sockfd);
	double elapsed = timeDifference(TimeStamp::now(), start);
	printf("%.3f seconds\n%.3f MiB/s\n", elapsed, totalMb / elapsed);
}

void TTcp::blockReceive(const Options& opt)
{
	int sockfd = acceptOrDie(opt.port);
	struct SessionMessage sessionMessage = { 0,0 };
	if (read(sockfd, &sessionMessage, sizeof(sessionMessage)) != sizeof(sessionMessage))
	{
		perror("read SessionMessage");
		exit(1);
	}

	sessionMessage.number = ntohl(sessionMessage.number);
	sessionMessage.length = ntohl(sessionMessage.length);
	printf("receive number = %d\nreceive length = %d\n",
		sessionMessage.number, sessionMessage.length);

	const int totalLen = static_cast<int>(sizeof(int32_t) + sessionMessage.length);
	PayloadMessage *payload = static_cast<PayloadMessage*>(zmalloc(totalLen));
	assert(payload);

	for (int i = 0; i < sessionMessage.number; ++i)
	{
		payload->length = 0;
		if (read(sockfd, &payload->length, sizeof(payload->length)) != sizeof(payload->length))
		{
			perror("read length");
			exit(1);
		}
		payload->length = ntohl(payload->length);
		assert(payload->length == sessionMessage.length);
		if (read(sockfd, payload->data, payload->length) != payload->length)
		{
			perror("read payload data");
			exit(1);
		}
		int32_t ack = htonl(payload->length);
		if (write(sockfd, &ack, sizeof(ack)) != sizeof(ack))
		{
			perror("write ack");
			exit(1);
		}
	}
	zfree(payload);
	::close(sockfd);

}

int TTcp::acceptOrDie(uint16_t port)
{
	int listenfd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	assert(listenfd >= 0);

	int yes = 1;
	if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)))
	{
		perror("setsockopt");
		exit(1);
	}

	struct sockaddr_in addr;
	bzero(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	addr.sin_addr.s_addr = INADDR_ANY;
	if (bind(listenfd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)))
	{
		perror("bind");
		exit(1);
	}

	if (listen(listenfd, 5))
	{
		perror("listen");
		exit(1);
	}

	struct sockaddr_in peer_addr;
	bzero(&peer_addr, sizeof(peer_addr));
	socklen_t addrlen = 0;
	int sockfd = ::accept(listenfd, reinterpret_cast<struct sockaddr*>(&peer_addr), &addrlen);
	if (sockfd < 0)
	{
		perror("accept");
		exit(1);
	}
	::close(listenfd);
	return sockfd;
}


