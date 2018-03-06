#include "xSession.h"
#include "xRedis.h"
#include "xMemcached.h"

const int kLongestKeySize = 250;
std::string xSession::kLongestKey(kLongestKeySize, 'x');

xSession::xSession(xRedis *redis,const TcpConnectionPtr &conn)
:reqtype(0),
 multibulklen(0),
 bulklen(-1),
 argc(0),
 clientConn(conn),
 redis(redis),
 authEnabled(false),
 replyBuffer(false),
 fromMaster(false),
 fromSlave(false)
{
	command = redis->object.createStringObject(nullptr, REDIS_COMMAND_LENGTH);
	clientConn->setMessageCallback(std::bind(&xSession::readCallBack, this, std::placeholders::_1,std::placeholders::_2));
}

xSession::xSession(xMemcached *memcahed,const TcpConnectionPtr &conn)
:memcached(memcached),
clientConn(conn),
state(kNewCommand),
protocol(kAscii),
noreply(false),
policy(xItem::kInvalid),
bytesToDiscard(0),
needle(xItem::makeItem(kLongestKey, 0, 0, 2, 0)),
bytesRead(0),
requestsProcessed(0)
{
	 clientConn->setMessageCallback(std::bind(&xSession::onMessage, this, std::placeholders::_1,std::placeholders::_2));
}

xSession::~xSession()
{
	if(sdslen(command->ptr))
	{
		zfree(command);
	}
}

void xSession::readCallBack(const TcpConnectionPtr &clientConn, xBuffer *recvBuf)
{
	while(recvBuf->readableBytes() > 0 )
	{
		if(!reqtype)
		{
			if((recvBuf->peek()[0])== '*')
			{
				reqtype = REDIS_REQ_MULTIBULK;
			}
			else
			{
				reqtype = REDIS_REQ_INLINE;
			}
		}

		if(reqtype == REDIS_REQ_MULTIBULK)
		{
			if(processMultibulkBuffer(recvBuf)!= REDIS_OK)
			{
				break;
			}
		}
		else if(reqtype == REDIS_REQ_INLINE)
		{
			if(processInlineBuffer(recvBuf)!= REDIS_OK)
			{
				break;
			}
		}
		else
		{
			LOG_WARN<<"Unknown request type";
		}

		processCommand();
		reset();

	}

	if(clientBuffer.readableBytes() > 0 )
	{
		clientConn->send(&clientBuffer);
		clientBuffer.retrieveAll();
	}

	if(pubsubBuffer.readableBytes() > 0 )
	{
		pubsubBuffer.retrieveAll();
	}

	if(redis->repliEnabled)
	{
		std::unique_lock <std::mutex> lck(redis->slaveMutex);
		for (auto &it : redis->slaveConns)
		{
			if (slaveBuffer.readableBytes() > 0)
			{
			 	it.second->send(&slaveBuffer);
			}
		}

		slaveBuffer.retrieveAll();
	}
}

bool xSession::checkCommand(rObj *commands)
{
	auto it = redis->commands.find(commands);
	if(it == redis->commands.end())
	{
		return false;
	}
		
	return true;
}


int32_t xSession::processCommand()
{
	if(redis->authEnabled)
	{
		if(!authEnabled)
		{
			if (strcasecmp(commands[0]->ptr,"auth") != 0)
			{
				clearObj();
				redis->object.addReplyErrorFormat(clientBuffer,"NOAUTH Authentication required");
				return REDIS_ERR;
			}
		}
	}

	if (redis->clusterEnabled)
	{
		if(redis->getClusterMap(command))
		{
			goto jump;
		}
			
		assert(!commands.empty());
		char * key = commands[0]->ptr;
		int32_t hashslot = redis->clus.keyHashSlot(key, sdslen(key));
		
		std::unique_lock <std::mutex> lck(redis->clusterMutex);
		if(redis->clusterRepliMigratEnabled)
		{
			auto &map = redis->clus.getMigrating();
			for(auto &it : map)
			{
				auto iter = it.second.find(hashslot);
				if(iter != it.second.end())
				{
					redis->structureRedisProtocol(redis->clusterMigratCached,commands);
					goto jump;
				}
			}
		}

		if(redis->clusterRepliImportEnabeld)
		{
			auto &map = redis->clus.getImporting();
			for(auto &it : map)
			{
				auto iter = it.second.find(hashslot);
				if(iter !=  it.second.end())
				{
					replyBuffer = true;
					goto jump;
				}
			}
		}

		auto it = redis->clus.checkClusterSlot(hashslot);
		if (it == nullptr)
		{
			redis->clus.clusterRedirectClient(shared_from_this(), nullptr, hashslot, CLUSTER_REDIR_DOWN_UNBOUND);
			clearObj();
			return REDIS_ERR;
		}
		else
		{
			if (redis->ip == it->ip && redis->port == it->port)
			{
				//FIXME
			}
			else
			{
				redis->clus.clusterRedirectClient(shared_from_this(), it, hashslot, CLUSTER_REDIR_MOVED);
				clearObj();
				return REDIS_ERR;
			}
		}
	}

jump:

	if(redis->repliEnabled)
	{
		if (clientConn->getSockfd() == redis->masterfd)
		{
			fromMaster = true;
			if(!checkCommand(command))
			{
				clearObj();
				LOG_WARN<<"master sync command unknow " << command;
				return REDIS_ERR;
			}
		}
		else if(redis->masterfd > 0)
		{
			if(checkCommand(command))
			{
				clearObj();
				redis->object.addReplyErrorFormat(clientBuffer,"slaveof command unknown");
				return REDIS_ERR;
			}
		}
 		else
		{
			if(checkCommand(command))
			{
				commands.push_front(command);

				{
					std::unique_lock <std::mutex> lck(redis->slaveMutex);
					if(redis->salveCount < redis->slaveConns.size())
					{
						redis->structureRedisProtocol(redis->slaveCached,commands);
					}
					else
					{
						redis->structureRedisProtocol(slaveBuffer,commands);
					}
				}
				commands.pop_front();
			}
		}

	}


	auto & map = redis->getHandlerCommandMap();
	auto it = map.find(command);
	if(it == map.end())
	{
		clearObj();
		redis->object.addReplyErrorFormat(clientBuffer,"command unknown");
		return REDIS_ERR;
	}

	if(!it->second(commands,shared_from_this()))
	{
		clearObj();
		return REDIS_ERR;
	}

	return REDIS_OK;
}

void xSession::resetVlaue()
{
	
}

void xSession::clearObj()
{
	for(auto &it : commands)
	{
		zfree(it);
	}	
}

void xSession::reset()
{
	argc = 0;
	multibulklen = 0;
	bulklen = -1;
	commands.clear();

	if(replyBuffer)
	{
		clientBuffer.retrieveAll();
	    replyBuffer = false;
	}

	if(fromMaster)
	{
		clientBuffer.retrieveAll();
		slaveBuffer.retrieveAll();
		pubsubBuffer.retrieveAll();
		fromMaster = false;
	}
}

int32_t xSession::processInlineBuffer(xBuffer *recvBuf)
{
    const char *newline;
	const char *queryBuf = recvBuf->peek();
	int32_t  j;
	size_t queryLen;
	sds *argv, aux;
	  
	newline = strchr(queryBuf,'\n');
	if(newline == nullptr)
	{
		 if(recvBuf->readableBytes() > PROTO_INLINE_MAX_SIZE)
		 {
			LOG_WARN << "Protocol error";
			redis->object.addReplyError(clientBuffer,"Protocol error: too big inline request");
			recvBuf->retrieveAll();
		 }
		 
		 return REDIS_ERR;
	}

	if (newline && newline != queryBuf && *(newline-1) == '\r')
	newline--;
	  
	queryLen = newline-(queryBuf);
	aux = sdsnewlen(queryBuf,queryLen);
	argv = sdssplitargs(aux,&argc);
	sdsfree(aux);

	recvBuf->retrieve(queryLen + 2);
	  
	if (argv == nullptr) 
	{
		LOG_WARN << "Protocol error";
		redis->object.addReplyError(clientBuffer,"Protocol error: unbalanced quotes in request");
		recvBuf->retrieveAll();
		return REDIS_ERR;
    }

	for (j = 0; j  < argc; j++)
	{
		if(j == 0)
		{
			sdscpylen((sds)(command->ptr),argv[j],sdslen(argv[j]));
			command->calHash();
		}
		else
		{
			rObj * obj = (rObj*)redis->object.createStringObject(argv[j],sdslen(argv[j]));
			obj->calHash();
			commands.push_back(obj);
		}
		sdsfree(argv[j]);
	}

	zfree(argv);
	return REDIS_OK;
   
}

int32_t xSession::processMultibulkBuffer(xBuffer *recvBuf)
{
	const char * newline = nullptr;
	int32_t pos = 0,ok;
	long long ll = 0 ;
	const char * queryBuf = recvBuf->peek();
	if(multibulklen == 0)
	{
		newline = strchr(queryBuf,'\r');
		if(newline == nullptr)
		{
			if(recvBuf->readableBytes() > REDIS_INLINE_MAX_SIZE)
			{
				redis->object.addReplyError(clientBuffer,"Protocol error: too big mbulk count string");
				LOG_INFO<<"Protocol error: too big mbulk count string";
				recvBuf->retrieveAll();
			}

			return REDIS_ERR;
		}


		  if (newline-(queryBuf) > ((signed)recvBuf->readableBytes()-2))
		  {
		  	 return REDIS_ERR;
		  }
     

		if(queryBuf[0] != '*')
		{
			LOG_WARN<<"Protocol error: *";
			recvBuf->retrieveAll();
			return REDIS_ERR;
		}

		ok = string2ll(queryBuf + 1,newline - ( queryBuf + 1),&ll);
		if(!ok || ll > 1024 * 1024)
		{
			redis->object.addReplyError(clientBuffer,"Protocol error: invalid multibulk length");
			LOG_INFO<<"Protocol error: invalid multibulk length";
			recvBuf->retrieveAll();
			return REDIS_ERR;
		}

		pos = (newline - queryBuf) + 2;
		if(ll <= 0)
		{
			recvBuf->retrieve(pos);
			return REDIS_OK;
		}

		multibulklen = ll;

	}
	
	while(multibulklen)
	{
		if(bulklen == -1)
		{
			newline = strchr(queryBuf + pos, '\r');
			if(newline == nullptr)
			{
				if(recvBuf->readableBytes() > REDIS_INLINE_MAX_SIZE)
				{
					redis->object.addReplyError(clientBuffer,"Protocol error: too big bulk count string");
					LOG_INFO<<"Protocol error: too big bulk count string";
					recvBuf->retrieveAll();
					return REDIS_ERR;
				}
			
				break;
			}

			if( (newline - queryBuf) > ((signed)recvBuf->readableBytes() - 2))
			{
				break;
			}

			if(queryBuf[pos] != '$')
			{
				redis->object.addReplyErrorFormat(clientBuffer,"Protocol error: expected '$', got '%c'",queryBuf[pos]);
				LOG_INFO<<"Protocol error: expected '$'";
				recvBuf->retrieveAll();
				return REDIS_ERR;
			}


			ok = string2ll(queryBuf + pos +  1,newline - (queryBuf + pos + 1),&ll);
			if(!ok || ll < 0 || ll > 512 * 1024 * 1024)
			{
				redis->object.addReplyError(clientBuffer,"Protocol error: invalid bulk length");
				LOG_INFO<<"Protocol error: invalid bulk length";
				recvBuf->retrieveAll();
				return REDIS_ERR;
			}

			pos += newline - (queryBuf + pos) + 2;
			if(ll >= REDIS_MBULK_BIG_ARG)
			{	
				return REDIS_ERR;
			}
			bulklen = ll;
		}

		if(recvBuf->readableBytes() - pos < (bulklen + 2))
		{
			break;
		}
		else
		{
			if(++argc == 1)
			{
				sdscpylen(command->ptr,queryBuf + pos,bulklen);
				command->calHash();
			}
			else
			{
				rObj * obj = (rObj*)redis->object.createStringObject((char*)(queryBuf + pos),bulklen);
				obj->calHash();
				commands.push_back(obj);
			}

			pos += bulklen+2;
			bulklen = -1;
			multibulklen --;
		}
	}

	
	if(pos)
	{
		recvBuf->retrieve(pos);
	}	
	
	if(multibulklen == 0)
	{
		return REDIS_OK;
	}

	return REDIS_ERR;
}


struct xSession::Reader
{
	Reader(auto  &beg, auto end)
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
 	std::vector<xStringPiece>::iterator  first;
	std::vector<xStringPiece>::iterator  last;
};


void xSession::receiveValue(xBuffer *buf)
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
			if (memcached->storeItem(currItem, policy, &exists))
			{
				reply("STORED\r\n");
			}
			else
			{
				if (policy == xItem::kCas)
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

void xSession::discardValue(xBuffer *buf)
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
		bytesToDiscard= 0;
		resetRequest();
		state = kNewCommand;
	}
}



bool xSession::processRequest(xStringPiece request)
{
	std::string command;
	assert(command.empty());
	assert(!noreply);
	assert(policy == xItem::kInvalid);
	assert(!currItem);
	assert(bytesToDiscard == 0);
	++requestsProcessed;

	if (request.size() >= 8)
	{
		xStringPiece end(request.end() - 8, 8);
		if (end == " noreply")
		{
			noreply = true;
			request.removeSuffix(8);
		}
	}

	std::vector<xStringPiece> tokenizers;
	const char *next = request.begin();
	const char *end = request.end();

	for(;;)
	{
		while (next != end && *next == ' ')
		++next;
		if (next == end)
		{
			break;
		}

		xStringPiece tok;
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

		return doUpdate(command,beg, tokenizers.end());
	}
	else if (command == "get" || command == "gets")
	{
		bool cas = command == "gets";
		while (beg != tokenizers.end())
		{
			xStringPiece key = *beg;
			bool good = key.size() <= kLongestKeySize;
			if (!good)
			{
				reply("CLIENT_ERROR bad command line format\r\n");
				return true;
			}

			needle->resetKey(key);
			ConstItemPtr item = memcached->getItem(needle);
			++beg;
			if (item)
			{
				item->output(&clientBuffer, cas);
			}
		}
		clientBuffer.append("END\r\n");

		if (clientConn->outputBuffer()->writableBytes() > 65536 + clientBuffer.readableBytes())
		{
			LOG_DEBUG << "shrink output buffer from " << clientConn->outputBuffer()->internalCapacity();
			clientConn->outputBuffer()->shrink(65536 + clientBuffer.readableBytes());
		}

		clientConn->send(&clientBuffer);
	}
	else if (command == "delete")
	{
		doDelete(command,beg, tokenizers.end());
	}
	else if (command == "version")
	{
		reply("VERSION 0.01 memcached \r\n");
	}
	else if (command == "quit")
	{
		clientConn->shutdown();
	}
	else if (command == "shutdown")
	{
		clientConn->shutdown();
		memcached->stop();
	}
	else
	{
		reply("ERROR\r\n");
		LOG_INFO << "Unknown command: " << command;
	}
 	return true;
}

void xSession::resetRequest()
{
	noreply = false;
	policy = xItem::kInvalid;
	currItem.reset();
	bytesToDiscard = 0;
}

void xSession::reply(xStringPiece msg)
{
	if (!noreply)
	{
		clientConn->send(msg.data(), msg.size());
	}
}

bool xSession::doUpdate(const std::string &command,auto &beg, auto end)
{
	if (command == "set")
		policy = xItem::kSet;
	else if (command == "add")
		policy = xItem::kAdd;
	else if (command == "replace")
		policy = xItem::kReplace;
	else if (command == "append")
		policy = xItem::kAppend;
	else if (command == "prepend")
		policy = xItem::kPrepend;
	else if (command == "cas")
		policy = xItem::kCas;
	else
	assert(false);

	xStringPiece key = (*beg);
	++beg;
	bool good = key.size() <= kLongestKeySize;

	uint32_t flags = 0;
	time_t exptime = 1;
	int bytes = -1;
	uint64_t cas = 0;

	Reader r(beg, end);
	good = good && r.read(&flags) && r.read(&exptime) && r.read(&bytes);

	int relExptime = static_cast<int>(exptime);
	if (exptime > 60*60*24*30)
	{
		relExptime = static_cast<int>(exptime - memcached->getStartTime());
		if (relExptime < 1)
		{
			relExptime = 1;
		}
	}
	else
	{

		// relExptime = exptime + currentTime;
	}

	if (good && policy == xItem::kCas)
	{
		good = r.read(&cas);
	}

	if (!good)
	{
		reply("CLIENT_ERROR bad command line format\r\n");
		return true;
	}

	if (bytes > 1024*1024)
	{
		reply("SERVER_ERROR object too large for cache\r\n");
		needle->resetKey(key);
		memcached->deleteItem(needle);
		bytesToDiscard = bytes + 2;
		state = kDiscardValue;
		return false;
	}
	else
	{
		currItem = xItem::makeItem(key, flags, relExptime, bytes + 2, cas);
		state = kReceiveValue;
		return false;
	}
}

void xSession::doDelete(const std::string &command,auto  &beg, auto end)
{
	assert(command == "delete");
	xStringPiece key = *beg;
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
		if (memcached->deleteItem(needle))
		{
			reply("DELETED\r\n");
		}
		else
		{
			reply("NOT_FOUND\r\n");
		}
	}
}

static bool isBinaryProtocol(uint8_t firstByte)
{
	return firstByte == 0x80;
}

void xSession::onMessage(const TcpConnectionPtr &conn,xBuffer *buf)
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
					xStringPiece request(buf->peek(), len);
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
						clientConn->shutdown();
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




