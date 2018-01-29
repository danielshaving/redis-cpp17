#include "eventloop.h"

eventLoop::eventLoop()
:enabled(true)
{

}



eventLoop::~eventLoop()
{

}


void eventLoop::pushSession(TcpService::SESSION_TYPE type)
{
	std::unique_lock <std::mutex> lck(mtx);
	disconnectSessions.push_back(type);
}

void eventLoop::pushMsg(const std::shared_ptr<packet> &p)
{
	std::unique_lock <std::mutex> lck(mtx);
	packetDeques.push_back(p);
}


void eventLoop::run()
{
	while(enabled)
	{

		{
			std::unique_lock <std::mutex> lck(mtx);
			if(!packetDeques.empty())
			{
				packetDeque.swap(packetDeques);
				lck.unlock();
				for(auto it = packetDeque.begin(); it != packetDeque.end(); ++it)
				{
					auto iter = handlers.find((*it)->id);
					if(iter != handlers.end())
					{
						iter->second((*it));
					}
				}

				packetDeques.clear();
				packetDeque.clear();

			}
		}

		{
			std::unique_lock <std::mutex> lck(mtx);
			if(!disconnectSessions.empty())
			{
				disconnectSession.swap(disconnectSessions);
				lck.unlock();
				for(auto it = disconnectSession.begin(); it != disconnectSession.end(); ++it)
				{
					auto iter = sessions.find((*it));
					if(iter != sessions.end())
					{
						players.erase(iter->second);
					}
				}

				disconnectSessions.clear();
				disconnectSession.clear();

			}
		}

		std::chrono::milliseconds timespan(2000); // or whatever
		std::this_thread::sleep_for(timespan);
	}


}


