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
	cmd = createStringObject(nullptr,REDIS_COMMAND_LENGTH);
	clientConn->setMessageCallback(std::bind(&Session::readCallBack,
			this,std::placeholders::_1,std::placeholders::_2));
}

Session::~Session()
{

}


/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process. */

void Session::readCallBack(const TcpConnectionPtr &clientConn,Buffer *buffer)
{
	/* Keep processing while there is something in the input buffer */
	while(buffer->readableBytes() > 0 )
	{
	     /* Determine request type when unknown. */
		if (!reqtype)
		{
			if ((buffer->peek()[0])== '*')
			{
				reqtype = REDIS_REQ_MULTIBULK;
			}
			else
			{
				reqtype = REDIS_REQ_INLINE;
			}
		}

		if (reqtype == REDIS_REQ_MULTIBULK)
		{
			if (processMultibulkBuffer(buffer) != REDIS_OK)
			{
				break;
			}
		}
		else if (reqtype == REDIS_REQ_INLINE)
		{
			if (processInlineBuffer(buffer) != REDIS_OK)
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

	/* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
	if (clientBuffer.readableBytes() > 0 )
	{
		clientConn->send(&clientBuffer);
		clientBuffer.retrieveAll();
	}

	if (pubsubBuffer.readableBytes() > 0 )
	{
		pubsubBuffer.retrieveAll();
	}

	if (redis->repliEnabled)
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

/* Only reset the client when the command was executed. */
int32_t Session::processCommand()
{
	if (redis->authEnabled)
	{
		if (!authEnabled)
		{
			if (strcasecmp(redisCommands[0]->ptr,"auth") != 0)
			{
				clearObj();
				addReplyErrorFormat(clientBuffer,"NOAUTH Authentication required");
				return REDIS_ERR;
			}
		}
	}

	if (redis->clusterEnabled)
	{
		if (redis->getClusterMap(cmd))
		{
			goto jump;
		}
			
		assert(!redisCommands.empty());
		char *key = redisCommands[0]->ptr;
		int32_t hashslot = redis->getCluster()->keyHashSlot(key,sdslen(key));
		
		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		if (redis->clusterRepliMigratEnabled)
		{
			auto &map = redis->getCluster()->getMigrating();
			for(auto &it : map)
			{
				auto iter = it.second.find(hashslot);
				if (iter != it.second.end())
				{
					redis->structureRedisProtocol(redis->clusterMigratCached,redisCommands);
					goto jump;
				}
			}
		}

		if (redis->clusterRepliImportEnabeld)
		{
			auto &map = redis->getCluster()->getImporting();
			for(auto &it : map)
			{
				auto iter = it.second.find(hashslot);
				if (iter !=  it.second.end())
				{
					replyBuffer = true;
					goto jump;
				}
			}
		}

		auto it = redis->getCluster()->checkClusterSlot(hashslot);
		if (it == nullptr)
		{
			redis->getCluster()->clusterRedirectClient(shared_from_this(),
					nullptr,hashslot,CLUSTER_REDIR_DOWN_UNBOUND);
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
				redis->getCluster()->clusterRedirectClient(shared_from_this(),
						it,hashslot,CLUSTER_REDIR_MOVED);
				clearObj();
				return REDIS_ERR;
			}
		}
	}

jump:

	if (redis->repliEnabled)
	{
		if (clientConn->getSockfd() == redis->masterfd)
		{
			fromMaster = true;

			if (!redis->checkCommand(cmd))
			{
				clearObj();
				return REDIS_ERR;
			}
		}
		else if (redis->masterfd > 0)
		{
			if (redis->checkCommand(cmd))
			{
				clearObj();
				addReplyErrorFormat(clientBuffer,"slaveof cmd unknown");
				return REDIS_ERR;
			}
		}
 		else
		{
			if (redis->checkCommand(cmd))
			{
				redisCommands.push_front(cmd);
				{
					std::unique_lock <std::mutex> lck(redis->getSlaveMutex());
					if (redis->salveCount < redis->getSlaveConn().size())
					{
						redis->structureRedisProtocol(redis->slaveCached,redisCommands);
					}
					else
					{
						redis->structureRedisProtocol(slaveBuffer,redisCommands);
					}
				}
				redisCommands.pop_front();
			}
		}
	}

	auto &handlerCommands = redis->getHandlerCommandMap();
	auto it = handlerCommands.find(cmd);
	if (it == handlerCommands.end())
	{
		clearObj();
		addReplyErrorFormat(clientBuffer,"unknown command `%s`,with args beginning",cmd->ptr);
		return REDIS_ERR;
	}

	it->second(redisCommands,shared_from_this());
	clearObj();
	return REDIS_OK;
}

void Session::resetVlaue()
{
	
}

void Session::clearObj()
{
	redisCommands.clear();
}

void Session::reset()
{
	argc = 0;
	multibulklen = 0;
	bulklen = -1;
	redisCommands.clear();

	if (replyBuffer)
	{
		clientBuffer.retrieveAll();
	    replyBuffer = false;
	}

	if (fromMaster)
	{
		clientBuffer.retrieveAll();
		slaveBuffer.retrieveAll();
		pubsubBuffer.retrieveAll();
		fromMaster = false;
	}
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure. Returns C_OK if the command
 * is ready to be executed, or C_ERR if there is still protocol to read to
 * have a well formed command. The function also returns C_ERR when there is
 * a protocol error: in such a case the client structure is setup to reply
 * with the error and close the connection. */

int32_t Session::processInlineBuffer(Buffer *buffer)
{
    const char *newline;
	const char *queryBuf = buffer->peek();
	int32_t j;
	size_t queryLen;
	sds *argv, aux;
	 
	/* Search for end of line */
	newline = strchr(queryBuf,'\n');

	 /* Nothing to do without a \r\n */
	if (newline == nullptr)
	{
		 if (buffer->readableBytes() > PROTO_INLINE_MAX_SIZE)
		 {
			LOG_WARN << "Protocol error";
			addReplyError(clientBuffer,"Protocol error: too big inline request");
			buffer->retrieveAll();
		 }
		 return REDIS_ERR;
	}

	/* Handle the \r\n case. */
	if (newline && newline != queryBuf && *(newline-1) == '\r')
	newline--;
	
	/* Split the input buffer up to the \r\n */
	queryLen = newline-(queryBuf);
	aux = sdsnewlen(queryBuf,queryLen);
	argv = sdssplitargs(aux,&argc);
	sdsfree(aux);

	/* retrieve cuurent buffer index */
	buffer->retrieve(queryLen + 2);
	  
	if (argv == nullptr) 
	{
		addReplyError(clientBuffer,"Protocol error: unbalanced quotes in request");
		buffer->retrieveAll();
		return REDIS_ERR;
    }

	/* Create redis objects for all arguments. */
	for (j = 0; j < argc; j++)
	{
		if (j == 0)
		{
			sdscpylen((sds)(cmd->ptr),argv[j],sdslen(argv[j]));
			cmd->calHash();
		}
		else
		{
			RedisObjectPtr obj = createStringObject(argv[j],sdslen(argv[j]));
			redisCommands.push_back(obj);
		}
		sdsfree(argv[j]);
	}

	zfree(argv);
	return REDIS_OK;
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution. Returns C_OK if after running the function
 * the client has a well-formed ready to be processed command, otherwise
 * C_ERR if there is still to read more buffer to get the full command.
 * The function also returns C_ERR when there is a protocol error: in such a
 * case the client structure is setup to reply with the error and close
 * the connection.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */

int32_t Session::processMultibulkBuffer(Buffer *buffer)
{
	const char *newline = nullptr;
	int32_t pos = 0,ok;
	int64_t ll = 0 ;
	const char *queryBuf = buffer->peek();
	if (multibulklen == 0)
	{
		/* Multi bulk length cannot be read without a \r\n */
		newline = strchr(queryBuf,'\r');
		if (newline == nullptr)
		{
			if (buffer->readableBytes() > REDIS_INLINE_MAX_SIZE)
			{
				addReplyError(clientBuffer,"Protocol error: too big mbulk count string");
				buffer->retrieveAll();
			}
			return REDIS_ERR;
		}

		/* Buffer should also contain \n */
		if (newline-(queryBuf) > ((signed)buffer->readableBytes()-2))
		{
			return REDIS_ERR;
		}

		if (queryBuf[0] != '*')
		{
			LOG_WARN<<"Protocol error: *";
			buffer->retrieveAll();
			return REDIS_ERR;
		}

		/* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
		ok = string2ll(queryBuf + 1,newline - ( queryBuf + 1),&ll);
		if (!ok || ll > 1024 * 1024)
		{
			addReplyError(clientBuffer,"Protocol error: invalid multibulk length");
			buffer->retrieveAll();
			return REDIS_ERR;
		}

		pos = (newline - queryBuf) + 2;
		if (ll <= 0)
		{
			buffer->retrieve(pos);
			return REDIS_OK;
		}
		multibulklen = ll;
	}
	
	while(multibulklen)
	{
		/* Read bulk length if unknown */
		if (bulklen == -1)
		{
			newline = strchr(queryBuf + pos, '\r');
			if (newline == nullptr)
			{
				if (buffer->readableBytes() > REDIS_INLINE_MAX_SIZE)
				{
					addReplyError(clientBuffer,"Protocol error: too big bulk count string");
					buffer->retrieveAll();
					return REDIS_ERR;
				}
			
				break;
			}

			/* Buffer should also contain \n */
			if ( (newline - queryBuf) > ((signed)buffer->readableBytes() - 2))
			{
				break;
			}

			if (queryBuf[pos] != '$')
			{
				addReplyErrorFormat(clientBuffer,"Protocol error: expected '$', got '%c'",queryBuf[pos]);
				buffer->retrieveAll();
				return REDIS_ERR;
			}

			ok = string2ll(queryBuf + pos + 1,newline - (queryBuf + pos + 1),&ll);
			if (!ok || ll < 0 || ll > 512 * 1024 * 1024)
			{
				addReplyError(clientBuffer,"Protocol error: invalid bulk length");
				buffer->retrieveAll();
				return REDIS_ERR;
			}

			pos += newline - (queryBuf + pos) + 2;
			if (ll >= REDIS_MBULK_BIG_ARG)
			{	
				return REDIS_ERR;
			}
			bulklen = ll;
		}

		/* Read bulk argument */
		if (buffer->readableBytes() - pos < (bulklen + 2))
		{
			break;
		}
		else
		{
			/* Optimization: if the buffer contains JUST our bulk element
			* instead of creating a new object by *copying* the sds we
			* just use the current sds string. */
			if (++argc == 1)
			{
				sdscpylen(cmd->ptr,queryBuf + pos,bulklen);
				cmd->calHash();
			}
			else
			{
				RedisObjectPtr obj = createStringObject((char*)(queryBuf + pos),bulklen);
				redisCommands.push_back(obj);
			}

			pos += bulklen+2;
			bulklen = -1;
			multibulklen --;
		}
	}
	
	/* Trim to pos */
	if (pos)
	{
		buffer->retrieve(pos);
	}	
	
	/* We're done when c->multibulk == 0 */
	if (multibulklen == 0)
	{
		return REDIS_OK;
	}

	 /* Still not ready to process the command */
	return REDIS_ERR;
}







