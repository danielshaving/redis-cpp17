#include "xTimer.h"

xTimer::xTimer(xTimerCallback && cb, xTimestamp && expiration,bool	repeat,double interval,void * data)
:index(-1),
 repeat(repeat),
 interval(interval),
 expiration(std::move(expiration)),
callback(std::move(cb)),
data(data)
{
	
}


xTimer::~xTimer()
{

}


void xTimer::run()
{
	if(callback)
	{
		callback(data);
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



