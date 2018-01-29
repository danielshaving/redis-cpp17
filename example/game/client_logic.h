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

struct  packet;
class clientLogic
{
public:
	bool handlerClientLogic(const std::shared_ptr<packet> &content);
};
