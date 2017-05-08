#include "xTimer.h"

xTimer::xTimer(xTimerCallback && cb,xTimestamp && expiration,bool type)
:index(-1),
 type(type),
 expiration(std::move(expiration)),
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
