#include "xTimer.h"

xTimer::xTimer(xTimerCallback && cb, xTimestamp && expiration,bool	repeat,double interval)
:index(-1),
 repeat(repeat),
 interval(interval),
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


void xTimer::restart(xTimestamp now)
{
  if (repeat)
  {
    expiration = addTime(now, interval);
  }
  else
  {
    expiration = xTimestamp::invalid();
  }
}



