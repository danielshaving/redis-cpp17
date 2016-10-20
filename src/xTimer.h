#pragma once
#include "all.h"
#include "xCallback.h"
class xTimer
{
public:
	xTimer();
	xTimer(xTimerCallback && cb,int64_t value,int64_t key,int8_t type);
	~xTimer();
	void run();
public:	
	int32_t index;
	int64_t when;
	int64_t key;
	int8_t  type;
	xTimerCallback callback;
};


