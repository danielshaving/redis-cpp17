#include "xTimer.h"
xTimer::xTimer(xTimerCallback && cb,int64_t when,int64_t key,int8_t type)
:callback(std::move(cb)),
when(when),
index(-1),
key(key),
type(type)
{
	func = 100;
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


