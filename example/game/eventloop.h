#include <errno.h>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <functional>
#include <algorithm>
#include <memory>
#include <condition_variable>
#include <thread>
#include <chrono>


#include "common.h"
#include "zmalloc.h"
#include "client_logic.h"
#include "player.h"

class eventLoop
{
public:
	eventLoop();
	~eventLoop();

	void pushMsg(const std::shared_ptr<packet> &p);
	void pushSession(TcpService::SESSION_TYPE type);
	void run();

private:
	std::deque<std::shared_ptr<packet>> packetDeques;
	std::deque<std::shared_ptr<packet>> packetDeque;
	std::deque<TcpService::SESSION_TYPE> disconnectSessions;
	std::deque<TcpService::SESSION_TYPE> disconnectSession;

	typedef std::function<bool(std::shared_ptr<packet> &)> Func;
	std::unordered_map<int32_t,Func> handlers;
	std::unordered_map<int64_t,player> players;
	std::unordered_map<TcpService::SESSION_TYPE,int64_t> sessions;
	clientLogic clogic;
	std::mutex mtx;
	bool enabled;
};
