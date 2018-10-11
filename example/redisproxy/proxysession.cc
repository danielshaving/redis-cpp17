#include "proxysession.h"
#include "redisproxy.h"
#include "socket.h"

ProxySession::ProxySession(RedisProxy *redis, const TcpConnectionPtr &conn)
	:redis(redis),
	reqtype(0),
	multibulklen(0),
	bulklen(-1),
	argc(0),
	pos(0)
{
	command = createStringObject(nullptr, REDIS_COMMAND_LENGTH);
	conn->setMessageCallback(std::bind(&ProxySession::proxyReadCallback,
		this, std::placeholders::_1, std::placeholders::_2));
}

ProxySession::~ProxySession()
{

}

/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process. */

void ProxySession::proxyReadCallback(const TcpConnectionPtr &conn, Buffer *buffer)
{
	/* Keep processing while there is something in the input buffer */
	while (buffer->readableBytes() > 0)
	{
		if (!reqtype)
		{
			/* Determine request type when unknown. */
			if ((buffer->peek()[0]) == '*')
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
		if (redisCommands.empty())
		{
			redis->processCommand(conn, buf, len);
		}
		else
		{
			redisCommands.push_back(command);
			if (!redis->handleRedisCommand(command, shared_from_this(), redisCommands, conn))
			{
				addReplyErrorFormat(conn->outputBuffer(),
					"wrong number of arguments`%s`, for command", command->ptr);
			}
		}
		reset();
	}

	/* If there already are entries in the reply list, we cannot
		 * add anything more to the static buffer. */
	if (conn->outputBuffer()->readableBytes() > 0)
	{
		conn->sendPipe();
	}
}

void ProxySession::reset()
{
	reqtype = 0;
	argc = 0;
	multibulklen = 0;
	bulklen = -1;
	redisCommands.clear();
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure. Returns C_OK if the command
 * is ready to be executed, or C_ERR if there is still protocol to read to
 * have a well formed command. The function also returns C_ERR when there is
 * a protocol error: in such a case the client structure is setup to reply
 * with the error and close the connection. */

int32_t ProxySession::processInlineBuffer(const TcpConnectionPtr &conn, Buffer *buffer)
{
	const char *newline;
	const char *queryBuf = buffer->peek();
	int32_t j, linefeedChars = 1;
	int32_t queryLen;
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
	if (queryLen + linefeedChars > buffer->readableBytes())
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
	buf = queryBuf;
	len = queryLen + linefeedChars;

	bool get = false;
	/* Create redis objects for all arguments. */
	for (j = 0; j < argc; j++)
	{
		if (j == 0)
		{
			command->ptr = sdscpylen(command->ptr, argv[j], sdslen(argv[j]));
			command->calHash();
		}
		else
		{
			if (redis->getRedisCommand(command))
			{
				RedisObjectPtr obj = createStringObject(argv[j], sdslen(argv[j]));
				redisCommands.push_back(obj);
			}
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

int32_t ProxySession::processMultibulkBuffer(const TcpConnectionPtr &conn, Buffer *buffer)
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
			assert(false);
			addReplyError(conn->outputBuffer(), "Protocol error: *");
			conn->shutdown();
			return REDIS_ERR;
		}

		/* We know for sure there is a whole line since newline != NULL,
		 * so go ahead and find out the multi bulk length. */
		ok = string2ll(queryBuf + pos + 1, newline - (queryBuf + pos + 1), &ll);
		if (!ok || ll > REDIS_MBULK_BIG_ARG || ll <= 0)
		{
			assert(false);
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
				assert(false);
				addReplyErrorFormat(conn->outputBuffer(),
					"Protocol error: expected '$',got '%c'", queryBuf[pos]);
				conn->shutdown();
				return REDIS_ERR;
			}

			ok = string2ll(queryBuf + pos + 1, newline - (queryBuf + pos + 1), &ll);
			if (!ok || ll < 0 || ll > REDIS_MBULK_BIG_ARG)
			{
				assert(false);
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
				command->ptr = sdscpylen(command->ptr, queryBuf + pos, bulklen);
				command->calHash();
			}
			else
			{
				if (redis->getRedisCommand(command))
				{
					RedisObjectPtr obj = createStringObject((char*)queryBuf + pos, bulklen);
					redisCommands.push_back(obj);
				}
			}

			pos += bulklen + 2;
			bulklen = -1;
			multibulklen--;
		}
	}

	/* We're done when c->multibulk == 0 */
	if (multibulklen == 0)
	{
		buf = queryBuf;
		len = pos;

		/* Trim to pos */
		assert(pos <= buffer->readableBytes());
		buffer->retrieve(pos);
		pos = 0;
		return REDIS_OK;
	}

	/* Still not ready to process the command */
	return REDIS_ERR;
}
