#include "xTimer.h"

xTimer::xTimer()
{
	
}

xTimer::xTimer(xTimerCallback && cb,int64_t when,int64_t key,int8_t type)
:index(-1),
when(when),
key(key),
type(type),
callback(std::move(cb))
{
	
}


xTimer::~xTimer()
{

}


void xTimer::run()
{
	if(callback)
	{
		callback();	
	}
	
	
}


