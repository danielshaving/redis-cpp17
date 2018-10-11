#include "server.h"

Item::Item(std::string_view keyArg, uint32_t flagsArg,
	int exptimeArg, int valuelen, uint64_t casArg)
	:keyLen(keyArg.size()),
	flags(flagsArg),
	relExptime(exptimeArg),
	valueLen(valuelen),
	receivedBytes(0),
	cas(casArg),
	hash(dictGenHashFunction(keyArg.data(), keyArg.size())),
	data(static_cast<char*>(zmalloc(totalLen())))
{
	assert(valuelen >= 2);
	assert(receivedBytes < totalLen());
	append(keyArg.data(), keyLen);
}

size_t Item::neededBytes() const
{
	return totalLen() - receivedBytes;
}

void Item::append(const char *data, size_t len)
{
	assert(len <= neededBytes());
	memcpy(this->data + receivedBytes, data, len);
	receivedBytes += static_cast<int>(len);
	assert(receivedBytes <= totalLen());
}

void Item::output(Buffer *out, bool needCas) const
{
	out->append("VALUE ");
	out->append(data, keyLen);
	xLogStream buf;
	buf << ' ' << getFlags() << ' ' << valueLen - 2;
	if (needCas)
	{
		buf << ' ' << getCas();
	}

	buf << "\r\n";
	out->append(buf.getBuffer().getData(), buf.getBuffer().length());
	out->append(value(), valueLen);
}

void Item::resetKey(std::string_view k)
{
	assert(k.size() <= 250);
	keyLen = k.size();
	receivedBytes = 0;
	append(k.data(), k.size());
	hash = dictGenHashFunction(k.data(), k.size());
}

static bool isBinaryProtocol(uint8_t firstByte)
{
	return firstByte == 0x80;
}

struct Connect::Reader
{
	Reader(std::vector<std::string_view>::iterator &beg, std::vector<std::string_view>::iterator end)
		: first(beg),
		last(end)
	{

	}

	template<typename T>
	bool read(T* val)
	{
		if (first == last) return false;
		char* end = nullptr;
		uint64_t x = strtoull((*first).data(), &end, 10);
		if (end == (*first).end())
		{
			*val = static_cast<T>(x);
			++first;
			return true;
		}

		return false;
	}

private:
	std::vector<std::string_view>::iterator first;
	std::vector<std::string_view>::iterator last;
};

void Connect::receiveValue(Buffer *buf)
{
	assert(currItem.get());
	assert(state == kReceiveValue);

	const size_t avail = std::min(buf->readableBytes(), currItem->neededBytes());
	assert(currItem.unique());
	currItem->append(buf->peek(), avail);
	buf->retrieve(avail);
	if (currItem->neededBytes() == 0)
	{
		if (currItem->endsWithCRLF())
		{
			bool exists = false;
			if (owner->storeItem(currItem, policy, &exists))
			{
				reply("STORED\r\n");
			}
			else
			{
				if (policy == Item::kCas)
				{
					if (exists)
					{
						reply("EXISTS\r\n");
					}
					else
					{
						reply("NOT_FOUND\r\n");
					}
				}
				else
				{
					reply("NOT_STORED\r\n");
				}
			}
		}
		else
		{
			reply("CLIENT_ERROR bad data chunk\r\n");
		}

		resetRequest();
		state = kNewCommand;
	}
}

void Connect::discardValue(Buffer *buf)
{
	assert(!currItem);
	assert(state == kDiscardValue);
	if (buf->readableBytes() < bytesToDiscard)
	{
		bytesToDiscard -= buf->readableBytes();
		buf->retrieveAll();
	}
	else
	{
		buf->retrieve(bytesToDiscard);
		bytesToDiscard = 0;
		resetRequest();
		state = kNewCommand;
	}
}

bool Connect::processRequest(std::string_view request)
{
	assert(command.empty());
	assert(!noreply);
	assert(policy == Item::kInvalid);
	assert(!currItem);
	assert(bytesToDiscard == 0);
	++requestsProcessed;

	if (request.size() >= 8)
	{
		std::string_view end(request.end() - 8, 8);
		if (end == " noreply")
		{
			noreply = true;
			request.removeSuffix(8);
		}
	}

	std::vector<std::string_view> tokenizers;
	const char *next = request.begin();
	const char *end = request.end();

	for (;;)
	{
		while (next != end && *next == ' ')
			++next;
		if (next == end)
		{
			break;
		}

		std::string_view tok;
		const char * start = next;
		const char* sp = static_cast<const char*>(memchr(start, ' ', end - start));
		if (sp)
		{
			tok.set(start, static_cast<int>(sp - start));
			next = sp;
		}
		else
		{
			tok.set(start, static_cast<int>(end - next));
			next = end;
		}
		tokenizers.push_back(std::move(tok));
	}

	auto beg = tokenizers.begin();
	if (beg == tokenizers.end())
	{
		reply("ERROR\r\n");
		return true;
	}

	(*beg).copyToString(&command);
	++beg;
	if (command == "set" || command == "add" || command == "replace"
		|| command == "append" || command == "prepend" || command == "cas")
	{

		return doUpdate(beg, tokenizers.end());
	}
	else if (command == "get" || command == "gets")
	{
		bool cas = command == "gets";
		while (beg != tokenizers.end())
		{
			std::string_view key = *beg;
			bool good = key.size() <= kLongestKeySize;
			if (!good)
			{
				reply("CLIENT_ERROR bad command line format\r\n");
				return true;
			}

			needle->resetKey(key);
			ConstItemPtr item = owner->getItem(needle);
			++beg;
			if (item)
			{
				item->output(&outputBuf, cas);
			}
		}
		outputBuf.append("END\r\n");

		if (conn->outputBuffer()->writableBytes() > 65536 + outputBuf.readableBytes())
		{
			LOG_DEBUG << "shrink output buffer from " << conn->outputBuffer()->internalCapacity();
			conn->outputBuffer()->shrink(65536 + outputBuf.readableBytes());
		}

		conn->send(&outputBuf);
	}
	else if (command == "delete")
	{
		doDelete(beg, tokenizers.end());
	}
	else if (command == "version")
	{
		reply("VERSION 0.01 memcached \r\n");
	}
	else if (command == "quit")
	{
		conn->shutdown();
	}
	else if (command == "shutdown")
	{
		conn->shutdown();
		owner->stop();
	}
	else
	{
		reply("ERROR\r\n");
		LOG_INFO << "Unknown command: " << command;
	}
	return true;
}

void Connect::resetRequest()
{
	command.clear();
	noreply = false;
	policy = Item::kInvalid;
	currItem.reset();
	bytesToDiscard = 0;
}


void Connect::reply(std::string_view msg)
{
	if (!noreply)
	{
		conn->send(msg.data(), msg.size());
	}
}


bool Connect::doUpdate(std::vector<std::string_view>::iterator &beg, std::vector<std::string_view>::iterator end)
{
	if (command == "set")
		policy = Item::kSet;
	else if (command == "add")
		policy = Item::kAdd;
	else if (command == "replace")
		policy = Item::kReplace;
	else if (command == "append")
		policy = Item::kAppend;
	else if (command == "prepend")
		policy = Item::kPrepend;
	else if (command == "cas")
		policy = Item::kCas;
	else
		assert(false);

	std::string_view key = (*beg);
	++beg;
	bool good = key.size() <= kLongestKeySize;

	uint32_t flags = 0;
	time_t exptime = 1;
	int bytes = -1;
	uint64_t cas = 0;

	Reader r(beg, end);
	good = good && r.read(&flags) && r.read(&exptime) && r.read(&bytes);

	int relExptime = static_cast<int>(exptime);
	if (exptime > 60 * 60 * 24 * 30)
	{
		relExptime = static_cast<int>(exptime - owner->getStartTime());
		if (relExptime < 1)
		{
			relExptime = 1;
		}
	}
	else
	{

		// relExptime = exptime + currentTime;
	}

	if (good && policy == Item::kCas)
	{
		good = r.read(&cas);
	}

	if (!good)
	{
		reply("CLIENT_ERROR bad command line format\r\n");
		return true;
	}

	if (bytes > 1024 * 1024)
	{
		reply("SERVER_ERROR object too large for cache\r\n");
		needle->resetKey(key);
		owner->deleteItem(needle);
		bytesToDiscard = bytes + 2;
		state = kDiscardValue;
		return false;
	}
	else
	{
		currItem = Item::makeItem(key, flags, relExptime, bytes + 2, cas);
		state = kReceiveValue;
		return false;
	}
}

void Connect::doDelete(std::vector<std::string_view>::iterator &beg, std::vector<std::string_view>::iterator end)
{
	assert(command == "delete");
	std::string_view key = *beg;
	bool good = key.size() <= kLongestKeySize;
	++beg;
	if (!good)
	{
		reply("CLIENT_ERROR bad command line format\r\n");
	}
	else if (beg != end && *beg != "0") // issue 108, old protocol
	{
		reply("CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]\r\n");
	}
	else
	{
		needle->resetKey(key);
		if (owner->deleteItem(needle))
		{
			reply("DELETED\r\n");
		}
		else
		{
			reply("NOT_FOUND\r\n");
		}
	}
}

void Connect::onMessage(const TcpConnectionPtr &conn, Buffer *buf, void *data)
{
	const size_t initialReadable = buf->readableBytes();

	while (buf->readableBytes() > 0)
	{
		if (state == kNewCommand)
		{
			if (protocol == kAuto)
			{
				assert(bytesRead == 0);
				protocol = isBinaryProtocol(buf->peek()[0]) ? kBinary : kAscii;
			}

			assert(protocol == kAscii || protocol == kBinary);
			if (protocol == kBinary)
			{
				// FIXME
			}
			else  // ASCII protocol
			{
				const char* crlf = buf->findCRLF();
				if (crlf)
				{
					int len = static_cast<int>(crlf - buf->peek());
					std::string_view request(buf->peek(), len);
					if (processRequest(request))
					{
						resetRequest();
					}
					buf->retrieveUntil(crlf + 2);
				}
				else
				{
					if (buf->readableBytes() > 1024)
					{
						conn->shutdown();
					}
					break;
				}
			}
		}
		else if (state == kReceiveValue)
		{
			receiveValue(buf);
		}
		else if (state == kDiscardValue)
		{
			discardValue(buf);
		}
		else
		{
			assert(false);
		}
	}
}

MemcacheServer::MemcacheServer(EventLoop *loop, const Options & op)
	:loop(loop),
	ops(op),
	startTime(time(0))
{
	server.setConnectionCallback(std::bind(&MemcacheServer::onConnection, this, std::placeholders::_1));
}

MemcacheServer::~MemcacheServer()
{

}

void MemcacheServer::init()
{
	server.init(loop, ops.ip, ops.port, nullptr);
}

void MemcacheServer::start()
{
	server.start();
}

void MemcacheServer::quit()
{
	loop->quit();
}

void MemcacheServer::stop()
{
	loop->runAfter(3.0, nullptr, false, std::bind(&MemcacheServer::quit, this));
}

bool MemcacheServer::storeItem(const ItemPtr &item, Item::UpdatePolicy policy, bool *exists)
{
	assert(item->neededBytes() == 0);
	std::mutex &mutex = shards[item->getHash() % kShards].mutex;
	ItemMap& items = shards[item->getHash() % kShards].items;
	std::unique_lock <std::mutex> lck(mutex);
	ItemMap::const_iterator it = items.find(item);
	*exists = it != items.end();
	if (policy == Item::kSet)
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
		if (policy == Item::kAdd)
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
		else if (policy == Item::kReplace)
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
		else if (policy == Item::kAppend || policy == Item::kPrepend)
		{
			if (*exists)
			{
				const ConstItemPtr &oldItem = *it;
				int newLen = static_cast<int>(item->valueLength() + oldItem->valueLength() - 2);
				ItemPtr newItem(Item::makeItem(item->getKey(),
					oldItem->getFlags(),
					oldItem->getRelExptime(),
					newLen,
					cas++));
				if (policy == Item::kAppend)
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
		else if (policy == Item::kCas)
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

ConstItemPtr MemcacheServer::getItem(const ConstItemPtr &key) const
{
	std::mutex & mutex = shards[key->getHash() % kShards].mutex;
	const ItemMap& items = shards[key->getHash() % kShards].items;
	std::unique_lock <std::mutex> lck(mutex);
	ItemMap::const_iterator it = items.find(key);
	return it != items.end() ? *it : ConstItemPtr();
}

bool MemcacheServer::deleteItem(const ConstItemPtr &key)
{
	std::mutex & mutex = shards[key->getHash() % kShards].mutex;
	ItemMap& items = shards[key->getHash() % kShards].items;
	std::unique_lock <std::mutex> lck(mutex);
	return items.erase(key) == 1;
}

void MemcacheServer::onConnection(const TcpConnectionPtr &conn)
{
	if (conn->connected())
	{
		SessionPtr session(new Connect(this, conn));
		std::unique_lock <std::mutex> lck(mtx);
		assert(sessions.find(conn->getSockfd()) == sessions.end());
		sessions[conn->getSockfd()] = session;
	}
	else
	{
		std::unique_lock <std::mutex> lck(mtx);
		assert(sessions.find(conn->getSockfd()) != sessions.end());
		sessions.erase(conn->getSockfd());
	}
}

int main(int argc, char* argv[])
{
	MemcacheServer::Options options;
	options.ip = "127.0.0.1";
	options.port = 11211;

	EventLoop loop;
	MemcacheServer memcache(&loop, options);
	memcache.init();
	memcache.setThreadNum(4);
	memcache.start();
	loop.run();
	return 0;
}


