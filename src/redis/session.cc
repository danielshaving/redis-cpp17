#include "session.h"
#include "redis.h"

Session::Session(Redis *redis, const TcpConnectionPtr &conn)
	:reqtype(0),
	multibulklen(0),
	bulklen(-1),
	argc(0),
	redis(redis),
	authEnabled(false),
	replyBuffer(false),
	fromMaster(false),
	fromSlave(false),
	pos(0)
{
	cmd = createStringObject(nullptr, REDIS_COMMAND_LENGTH);
	conn->setMessageCallback(std::bind(&Session::readCallback,
		this, std::placeholders::_1, std::placeholders::_2));
}

Session::~Session()
{

}

void Session::clearCommand()
{
	redisCommands.clear();
}

/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process. */

void Session::readCallback(const TcpConnectionPtr &conn, Buffer *buffer)
{
	/* Keep processing while there is something in the input buffer */
	while (buffer->readableBytes() > 0)
	{
		/* Determine request type when unknown. */
		if (!reqtype)
		{
			if ((buffer->peek()[pos]) == '*')
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
			if (processMultibulkBuffer(conn, buffer) != REDIS_OK)
			{
				break;
			}
		}
		else if (reqtype == REDIS_REQ_INLINE)
		{
			if (processInlineBuffer(conn, buffer) != REDIS_OK)
			{
				break;
			}
		}
		else
		{
			LOG_WARN << "Unknown request type";
		}
		
		assert(multibulklen == 0);
		processCommand(conn);
		reset();
	}

	/* If there already are entries in the reply list, we cannot
	 * add anything more to the static buffer. */
	if (conn->outputBuffer()->readableBytes() > 0)
	{
		conn->sendPipe();
	}

	if (pubsubBuffer.readableBytes() > 0)
	{
		pubsubBuffer.retrieveAll();
	}

	if (redis->repliEnabled)
	{
		std::unique_lock<std::mutex> lck(redis->getSlaveMutex());
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

void Session::setAuth(bool enbaled)
{
	authEnabled = enbaled;
}

/* Only reset the client when the command was executed. */
int32_t Session::processCommand(const TcpConnectionPtr &conn)
{
	if (redis->authEnabled)
	{
		if (!authEnabled)
		{
			if (STRCMP(redisCommands[0]->ptr, "auth") != 0)
			{
				addReplyErrorFormat(conn->outputBuffer(), "NOAUTH Authentication required");
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

		if (redisCommands.empty())
		{
			goto jump;
		}

		char *key = redisCommands[0]->ptr;
		int32_t hashslot = redis->getCluster()->keyHashSlot(key, sdslen(key));

		std::unique_lock <std::mutex> lck(redis->getClusterMutex());
		if (redis->clusterRepliMigratEnabled)
		{
			auto &map = redis->getCluster()->getMigrating();
			for (auto &it : map)
			{
				auto iter = it.second.find(hashslot);
				if (iter != it.second.end())
				{
					redis->structureRedisProtocol(redis->clusterMigratCached, redisCommands);
					goto jump;
				}
			}
		}

		if (redis->clusterRepliImportEnabeld)
		{
			auto &map = redis->getCluster()->getImporting();
			for (auto &it : map)
			{
				auto iter = it.second.find(hashslot);
				if (iter != it.second.end())
				{
					replyBuffer = true;
					goto jump;
				}
			}
		}

		auto it = redis->getCluster()->checkClusterSlot(hashslot);
		if (it == nullptr)
		{
			redis->getCluster()->clusterRedirectClient(conn, shared_from_this(),
				nullptr, hashslot, CLUSTER_REDIR_DOWN_UNBOUND);
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
				redis->getCluster()->clusterRedirectClient(conn, shared_from_this(),
					it, hashslot, CLUSTER_REDIR_MOVED);
				return REDIS_ERR;
			}
		}
	}

jump:

	if (redis->repliEnabled)
	{
		if (conn->getSockfd() == redis->masterfd)
		{
			fromMaster = true;

			if (!redis->checkCommand(cmd))
			{
				return REDIS_ERR;
			}
		}
		else if (redis->masterfd > 0)
		{
			if (redis->checkCommand(cmd))
			{
				addReplyErrorFormat(conn->outputBuffer(), "slaveof cmd unknown");
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
						redis->structureRedisProtocol(redis->slaveCached, redisCommands);
					}
					else
					{
						redis->structureRedisProtocol(slaveBuffer, redisCommands);
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
		addReplyErrorFormat(conn->outputBuffer(),
			"unknown command `%s`, with args beginning", cmd->ptr);
		return REDIS_ERR;
	}
	else
	{
		if (!it->second(redisCommands, shared_from_this(), conn))
		{
			addReplyErrorFormat(conn->outputBuffer(),
				"wrong number of arguments`%s`, for command", cmd->ptr);
		}
		else
		{
			if (redis->monitorEnabled)
			{
				redisCommands.push_back(cmd);
				redis->feedMonitor(redisCommands, conn->getSockfd());
			}
		}
	}
	return REDIS_OK;
}

void Session::resetVlaue()
{

}

void Session::reset()
{
	reqtype = 0;
	argc = 0;
	multibulklen = 0;
	bulklen = -1;
	redisCommands.clear();
	
	if (replyBuffer)
	{
		replyBuffer = false;
	}

	if (fromMaster)
	{
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

int32_t Session::processInlineBuffer(const TcpConnectionPtr &conn, Buffer *buffer)
{
	const char *newline;
	const char *queryBuf = buffer->peek();
	int32_t j, linefeedChars = 1;
	size_t queryLen;
	sds *argv, aux;
	/* Search for end of line */
	newline = strchr(queryBuf, '\n');

	/* Nothing to do without a \r\n */
	if (newline == nullptr)
	{
		return REDIS_ERR;
	}

	/* Handle the \r\n case. */
	if (newline && newline != queryBuf && *(newline - 1) == '\r')
		newline--, linefeedChars++;

	/* Split the input buffer up to the \r\n */
	queryLen = newline - queryBuf;
	if ((queryLen + linefeedChars) > buffer->readableBytes())
	{
		return REDIS_ERR;
	}

	aux = sdsnewlen(queryBuf, queryLen);
	argv = sdssplitargs(aux, &argc);
	sdsfree(aux);

	if (argv == nullptr)
	{
		addReplyError(conn->outputBuffer(), "Protocol error: unbalanced quotes in request");
		conn->shutdown();
		return REDIS_ERR;
	}

	/* Leave data after the first line of the query in the buffer */
	buffer->retrieve(queryLen + linefeedChars);

	/* Create redis objects for all arguments. */
	for (j = 0; j < argc; j++)
	{
		if (j == 0)
		{
			cmd->ptr = sdscpylen((sds)(cmd->ptr), argv[j], sdslen(argv[j]));
			if (cmd->ptr[0] >= 'A' && cmd->ptr[0] <= 'Z')
			{
				int len = sdslen(cmd->ptr);
				for (int i = 0; i < len; i++)
				{
					cmd->ptr[i] += 32;
				}
			}
			cmd->calHash();
		}
		else
		{
			RedisObjectPtr obj = createStringObject(argv[j], sdslen(argv[j]));
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

int32_t Session::processMultibulkBuffer(const TcpConnectionPtr &conn, Buffer *buffer)
{
	const char *newline = nullptr;
	int32_t ok;
	int64_t ll = 0;
	const char *queryBuf = buffer->peek();
	if (multibulklen == 0)
	{
		/* Multi bulk length cannot be read without a \r\n */
		newline = strchr(queryBuf + pos, '\r');
		if (newline == nullptr)
		{
			return REDIS_ERR;
		}

		/* Buffer should also contain \n */
		if ((newline - (queryBuf + pos)) > (buffer->readableBytes() - pos - 2))
		{
			return REDIS_ERR;
		}

		if (queryBuf[pos] != '*')
		{
			addReplyError(conn->outputBuffer(), "Protocol error: *");
			conn->shutdown();
			return REDIS_ERR;
		}

		/* We know for sure there is a whole line since newline != NULL,
		 * so go ahead and find out the multi bulk length. */
		ok = string2ll(queryBuf + pos + 1, newline - (queryBuf + pos + 1), &ll);
		if (!ok || ll > REDIS_MBULK_BIG_ARG || ll <= 0)
		{
			addReplyError(conn->outputBuffer(), "Protocol error: invalid multibulk length");
			conn->shutdown();
			return REDIS_ERR;
		}

		pos += (newline - (queryBuf + pos)) + 2;
		multibulklen = ll;
	}

	while (multibulklen)
	{
		/* Read bulk length if unknown */
		if (bulklen == -1)
		{
			newline = strchr(queryBuf + pos, '\r');
			if (newline == nullptr)
			{
				break;
			}

			/* Buffer should also contain \n */
			if ((newline - (queryBuf + pos)) > (buffer->readableBytes() - pos - 2))
			{
				return REDIS_ERR;
			}


			if (queryBuf[pos] != '$')
			{
				addReplyErrorFormat(conn->outputBuffer(),
					"Protocol error: expected '$',got '%c'", queryBuf[pos]);
				conn->shutdown();
				return REDIS_ERR;
			}

			ok = string2ll(queryBuf + pos + 1, newline - (queryBuf + pos + 1), &ll);
			if (!ok || ll < 0 || ll > REDIS_MBULK_BIG_ARG)
			{
				addReplyError(conn->outputBuffer(),
					"Protocol error: invalid bulk length");
				conn->shutdown();
				return REDIS_ERR;
			}

			pos += (newline - (queryBuf + pos)) + 2;
			bulklen = ll;
		}

		/* Read bulk argument */
		if ((buffer->readableBytes() - pos) < (bulklen + 2))
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
				cmd->ptr = sdscpylen(cmd->ptr, queryBuf + pos, bulklen);
				if (cmd->ptr[0] >= 'A' && cmd->ptr[0] <= 'Z')
				{
					int len = sdslen(cmd->ptr);
					for (int i = 0; i < len; i++)
					{
						cmd->ptr[i] += 32;
					}
				}
				cmd->calHash();
			}
			else
			{
				RedisObjectPtr obj = createStringObject((char*)(queryBuf + pos), bulklen);
				redisCommands.push_back(obj);
			}

			pos += bulklen + 2;
			bulklen = -1;
			multibulklen--;
		}
	}

	/* We're done when c->multibulk == 0 */
	if (multibulklen == 0)
	{
		/* Trim to pos */
		assert(pos <= buffer->readableBytes());
		buffer->retrieve(pos);
		pos = 0;
		return REDIS_OK;
	}

	/* Still not ready to process the command */
	return REDIS_ERR;
}







