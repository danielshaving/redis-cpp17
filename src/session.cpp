#include "session.h"
#include "redis.h"

Session::Session(Redis *redis,const TcpConnectionPtr &conn)
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
	command = createStringObject(nullptr,REDIS_COMMAND_LENGTH);
	clientConn->setMessageCallback(std::bind(&Session::readCallBack,this,std::placeholders::_1,std::placeholders::_2));
}


Session::~Session()
{
	if(sdslen(command->ptr))
	{
		zfree(command);
	}
}

void Session::readCallBack(const TcpConnectionPtr &clientConn,Buffer *buffer)
{
	while(buffer->readableBytes() > 0 )
	{
		if(!reqtype)
		{
			if((buffer->peek()[0])== '*')
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
			if(processMultibulkBuffer(buffer)!= REDIS_OK)
			{
				break;
			}
		}
		else if(reqtype == REDIS_REQ_INLINE)
		{
			if(processInlineBuffer(buffer)!= REDIS_OK)
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
		std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
		auto &slaveConns = redis->getSlaveConn();
		for (auto &it : slaveConns)
		{
			if (slaveBuffer.readableBytes() > 0)
			{
			 	it.second->send(&slaveBuffer);
			}
		}

		slaveBuffer.retrieveAll();
	}
}

bool Session::checkCommand(rObj *commands)
{
	auto it = redis->commands.find(commands);
	if(it == redis->commands.end())
	{
		return false;
	}
		
	return true;
}

int32_t Session::processCommand()
{
	if(redis->authEnabled)
	{
		if(!authEnabled)
		{
			if (strcasecmp(commands[0]->ptr,"auth") != 0)
			{
				clearObj();
				addReplyErrorFormat(clientBuffer,"NOAUTH Authentication required");
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
		int32_t hashslot = redis->getCluster()->keyHashSlot(key,sdslen(key));
		
		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		if(redis->clusterRepliMigratEnabled)
		{
			auto &map = redis->getCluster()->getMigrating();
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
			auto &map = redis->getCluster()->getImporting();
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

		auto it = redis->getCluster()->checkClusterSlot(hashslot);
		if (it == nullptr)
		{
			redis->getCluster()->clusterRedirectClient(shared_from_this(), nullptr, hashslot, CLUSTER_REDIR_DOWN_UNBOUND);
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
				redis->getCluster()->clusterRedirectClient(shared_from_this(), it, hashslot, CLUSTER_REDIR_MOVED);
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
				addReplyErrorFormat(clientBuffer,"slaveof command unknown");
				return REDIS_ERR;
			}
		}
 		else
		{
			if(checkCommand(command))
			{
				commands.push_front(command);

				{
					std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
					if(redis->salveCount < redis->getSlaveConn().size())
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


	auto &map = redis->getHandlerCommandMap();
	auto it = map.find(command);
	if(it == map.end())
	{
		clearObj();
		addReplyErrorFormat(clientBuffer,"command unknown");
		return REDIS_ERR;
	}

	if(!it->second(commands,shared_from_this()))
	{
		clearObj();
		return REDIS_ERR;
	}

	return REDIS_OK;
}

void Session::resetVlaue()
{
	
}

void Session::clearObj()
{
	for(auto &it : commands)
	{
		zfree(it);
	}	
}

void Session::reset()
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

int32_t Session::processInlineBuffer(Buffer *buffer)
{
    const char *newline;
	const char *queryBuf = buffer->peek();
	int32_t  j;
	size_t queryLen;
	sds *argv, aux;
	  
	newline = strchr(queryBuf,'\n');
	if(newline == nullptr)
	{
		 if(buffer->readableBytes() > PROTO_INLINE_MAX_SIZE)
		 {
			LOG_WARN << "Protocol error";
			addReplyError(clientBuffer,"Protocol error: too big inline request");
			buffer->retrieveAll();
		 }
		 
		 return REDIS_ERR;
	}

	if (newline && newline != queryBuf && *(newline-1) == '\r')
	newline--;
	  
	queryLen = newline-(queryBuf);
	aux = sdsnewlen(queryBuf,queryLen);
	argv = sdssplitargs(aux,&argc);
	sdsfree(aux);

	buffer->retrieve(queryLen + 2);
	  
	if (argv == nullptr) 
	{
		LOG_WARN << "Protocol error";
		addReplyError(clientBuffer,"Protocol error: unbalanced quotes in request");
		buffer->retrieveAll();
		return REDIS_ERR;
    }

	for (j = 0; j < argc; j++)
	{
		if(j == 0)
		{
			sdscpylen((sds)(command->ptr),argv[j],sdslen(argv[j]));
			command->calHash();
		}
		else
		{
			rObj * obj = (rObj*)createStringObject(argv[j],sdslen(argv[j]));
			obj->calHash();
			commands.push_back(obj);
		}
		sdsfree(argv[j]);
	}

	zfree(argv);
	return REDIS_OK;
   
}

int32_t Session::processMultibulkBuffer(Buffer *buffer)
{
	const char *newline = nullptr;
	int32_t pos = 0,ok;
	int64_t ll = 0 ;
	const char * queryBuf = buffer->peek();
	if(multibulklen == 0)
	{
		newline = strchr(queryBuf,'\r');
		if(newline == nullptr)
		{
			if(buffer->readableBytes() > REDIS_INLINE_MAX_SIZE)
			{
				addReplyError(clientBuffer,"Protocol error: too big mbulk count string");
				LOG_INFO<<"Protocol error: too big mbulk count string";
				buffer->retrieveAll();
			}

			return REDIS_ERR;
		}


		if (newline-(queryBuf) > ((signed)buffer->readableBytes()-2))
		{
			return REDIS_ERR;
		}

		if(queryBuf[0] != '*')
		{
			LOG_WARN<<"Protocol error: *";
			buffer->retrieveAll();
			return REDIS_ERR;
		}

		ok = string2ll(queryBuf + 1,newline - ( queryBuf + 1),&ll);
		if(!ok || ll > 1024 * 1024)
		{
			addReplyError(clientBuffer,"Protocol error: invalid multibulk length");
			LOG_INFO<<"Protocol error: invalid multibulk length";
			buffer->retrieveAll();
			return REDIS_ERR;
		}

		pos = (newline - queryBuf) + 2;
		if(ll <= 0)
		{
			buffer->retrieve(pos);
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
				if(buffer->readableBytes() > REDIS_INLINE_MAX_SIZE)
				{
					addReplyError(clientBuffer,"Protocol error: too big bulk count string");
					LOG_INFO<<"Protocol error: too big bulk count string";
					buffer->retrieveAll();
					return REDIS_ERR;
				}
			
				break;
			}

			if( (newline - queryBuf) > ((signed)buffer->readableBytes() - 2))
			{
				break;
			}

			if(queryBuf[pos] != '$')
			{
				addReplyErrorFormat(clientBuffer,"Protocol error: expected '$', got '%c'",queryBuf[pos]);
				LOG_INFO<<"Protocol error: expected '$'";
				buffer->retrieveAll();
				return REDIS_ERR;
			}


			ok = string2ll(queryBuf + pos +  1,newline - (queryBuf + pos + 1),&ll);
			if(!ok || ll < 0 || ll > 512 * 1024 * 1024)
			{
				addReplyError(clientBuffer,"Protocol error: invalid bulk length");
				LOG_INFO<<"Protocol error: invalid bulk length";
				buffer->retrieveAll();
				return REDIS_ERR;
			}

			pos += newline - (queryBuf + pos) + 2;
			if(ll >= REDIS_MBULK_BIG_ARG)
			{	
				return REDIS_ERR;
			}
			bulklen = ll;
		}

		if(buffer->readableBytes() - pos < (bulklen + 2))
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
				rObj *obj = (rObj*)createStringObject((char*)(queryBuf + pos),bulklen);
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
		buffer->retrieve(pos);
	}	
	
	if(multibulklen == 0)
	{
		return REDIS_OK;
	}

	return REDIS_ERR;
}







