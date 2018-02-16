#include "xMemcached.h"

xMemcachedServer::xMemcachedServer(xEventLoop *loop,const Options & op)
:loop(loop),
 server(loop,ops.ip,ops.port,nullptr),
ops(op),
startTime(time(0))
{
	server.setConnectionCallback(std::bind(&xMemcachedServer::onConnection,this,std::placeholders::_1));
}

void xMemcachedServer::start()
{
	server.start();
}


void xMemcachedServer::quit(const std::any &context)
{
	loop->quit();
}

void xMemcachedServer::run()
{
	loop->run();
}

void xMemcachedServer::stop()
{
	 loop->runAfter(3.0, nullptr,false,std::bind(&xMemcachedServer::quit,this,std::placeholders::_1));
}

bool xMemcachedServer::storeItem(const xItemPtr & item, xItem::UpdatePolicy policy, bool *exists)
{
	assert(item->neededBytes() == 0);
	std::mutex & mutex = shards[item->getHash() % kShards].mutex;
	ItemMap& items = shards[item->getHash() % kShards].items;
	std::unique_lock <std::mutex> lck(mutex);
	ItemMap::const_iterator it = items.find(item);
	*exists = it != items.end();
	if (policy == xItem::kSet)
	{
		item->setCas(cas++);
		if (*exists)
		{
			items.erase(it);
		}

		items.insert(item);
	}
	else
	{
		if (policy == xItem::kAdd)
		{
			if (*exists)
			{
				return false;
			}
			else
			{
				item->setCas(cas++);
				items.insert(item);
			}
		}
		else if (policy == xItem::kReplace)
		{
			if (*exists)
			{
				item->setCas(cas++);
				items.erase(it);
				items.insert(item);
			}
			else
			{
				return false;
			}
		}
		else if (policy == xItem::kAppend || policy == xItem::kPrepend)
		{
			if (*exists)
			{
				const xConstItemPtr& oldItem = *it;
				int newLen = static_cast<int>(item->valueLength() + oldItem->valueLength() - 2);
				xItemPtr newItem(xItem::makeItem(item->getKey(),
				                   oldItem->getFlags(),
				                   oldItem->getRelExptime(),
				                   newLen,
				                   cas++));
				if (policy == xItem::kAppend)
				{
					newItem->append(oldItem->value(), oldItem->valueLength() - 2);
					newItem->append(item->value(), item->valueLength());
				}
				else
				{
					newItem->append(item->value(), item->valueLength() - 2);
					newItem->append(oldItem->value(), oldItem->valueLength());
				}
				assert(newItem->neededBytes() == 0);
				assert(newItem->endsWithCRLF());
				items.erase(it);
				items.insert(newItem);
			  }
			  else
			  {
			  	return false;
			  }
		}
		else if (policy == xItem::kCas)
		{
			if (*exists && (*it)->getCas() == item->getCas())
			{
				item->setCas(cas++);
				items.erase(it);
				items.insert(item);
			}
			else
			{
				return false;
			}
		}
		else
		{
		  assert(false);
		}
	}
	return true;
}

xConstItemPtr xMemcachedServer::getItem(const xConstItemPtr & key) const
{
	std::mutex & mutex  = shards[key->getHash() % kShards].mutex;
	const ItemMap& items = shards[key->getHash() % kShards].items;
	std::unique_lock <std::mutex> lck(mutex);
	ItemMap::const_iterator it = items.find(key);
	return it != items.end() ? *it : xConstItemPtr();
}

bool xMemcachedServer::deleteItem(const xConstItemPtr & key)
{
	std::mutex & mutex= shards[key->getHash() % kShards].mutex;
	ItemMap& items = shards[key->getHash() % kShards].items;
	std::unique_lock <std::mutex> lck(mutex);
	return items.erase(key) == 1;
}


void xMemcachedServer::onConnection(const xTcpconnectionPtr & conn)
{
	if(conn->connected())
	{
		xSessionPtr session(new xSession(this,conn));
		std::unique_lock <std::mutex> lck(mtx);
		assert(sessionMaps.find(conn->getSockfd()) == sessionMaps.end());
		sessionMaps[conn->getSockfd()] = session;
	}
	else
	{
		std::unique_lock <std::mutex> lck(mtx);
		assert(sessionMaps.find(conn->getSockfd()) != sessionMaps.end());
		sessionMaps.erase(conn->getSockfd());
	}
}

