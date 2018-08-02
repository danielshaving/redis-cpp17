#pragma once

namespace evcpp
{
	class EventLoop
	{
	public:
		EventLoop()
		:base(::event_base_new())
		{
			assert(base != nullptr);
		}

		~EventLoop()
		{
			::event_base_free(base);
		}

		int loop()
		{
			return ::event_base_loop(base,0);
		}

		struct event_base *eventBase()
		{
			return base;
		}

	private:
		struct event_base* const base;
	};
}
