#pragma once
#include "all.h"

struct Options
{
	Options()
	: port(0),length(0),number(0),
	  transmit(false),receive(false),nodelay(false)
	{

	}

	uint16_t port;
	int32_t length;
	int32_t number;
	bool transmit,receive,nodelay;
	std::string host;
};

bool parseCommandLine(int argc,char *argv[],Options *opt);
struct sockaddr_in resolveOrDie(const char* host,uint16_t port);

struct SessionMessage
{
	int32_t number;
	int32_t length;
} __attribute__ ((__packed__));

struct PayloadMessage
{
	int32_t length;
	char data[0];
};

void transmit(const Options& opt);

void receive(const Options& opt);
