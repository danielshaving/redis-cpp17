#include "xSession.h"
#include "xRedis.h"

xSession::xSession(xRedis *redis,const xTcpconnectionPtr & conn)
:reqtype(0),
 multibulklen(0),
 bulklen(-1),
 argc(0),
 conn(conn),
 redis(redis)
{
	 conn->setMessageCallback(
	        std::bind(&xSession::readCallBack, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
}

xSession::~xSession()
{

}

void xSession::readCallBack(const xTcpconnectionPtr& conn, xBuffer* recvBuf,void *data)
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

		if(reqtype == REDIS_REQ_INLINE )
		{
			assert(false);
			addReplyError(sendBuf,"Unknown request type");
			conn->send(&sendBuf);
			recvBuf->retrieveAll();
			break;
		}

		if(processMultibulkBuffer(recvBuf)!= REDIS_OK)
		{
			break;
		}
		else
		{
			if (argc == 0)
			{
				reset();
			}
			else
			{
				processCommand();
				reset();
			}
			
		}
	}
	if(sendBuf.readableBytes() > 0 )
	{
		conn->send(&sendBuf);	
	}
}


int xSession::processCommand()
{
	auto iter = redis->handlerCommondMap.find(commond);
	if(iter == redis->handlerCommondMap.end() )
	{
		addReplyErrorFormat(sendBuf,"unknown command '%s'",commond.c_str());
		return REDIS_ERR;
	}

	if(!iter->second(robjs,this))
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}



void xSession::resetVlaue()
{
	
}
void xSession::reset()
{
	 argc = 0;
	 multibulklen = 0;
	 bulklen = -1;
	 commond.clear();
	 robjs.clear();
}



int xSession::processMultibulkBuffer(xBuffer *recvBuf)
{
	bool marks = true;
	const char * newline = nullptr;
	int pos = 0,ok;
	long long ll = 0 ;
	const char * queryBuf = recvBuf->peek();
	if(multibulklen == 0)
	{
		newline = strchr(queryBuf,'\r');
		if(newline == nullptr)
		{
			if(recvBuf->readableBytes() > REDIS_INLINE_MAX_SIZE)
			{
				assert(false);
				addReplyError(sendBuf,"Protocol error: too big mbulk count string");
			}
			return REDIS_ERR;
		}

		if((newline - queryBuf ) > ((signed)(recvBuf->readableBytes())-2))
		{
			return REDIS_ERR;
		}

		if(queryBuf[0] != '*')
		{
			assert(false);
		}

		ok = string2ll(queryBuf + 1,newline - ( queryBuf + 1),&ll);
		if(!ok || ll > 1024 * 1024)
		{
			assert(false);
			addReplyError(sendBuf,"Protocol error: invalid multibulk length");
			return REDIS_ERR;
		}

		pos = newline - queryBuf + 2;
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
					assert(false);
					return REDIS_OK;
				}
				marks = false;
				break;
			}

			if( (newline - queryBuf) > ((signed)(recvBuf->readableBytes()) - 2))
			{
				marks = false;
				break;
			}

			if(queryBuf[pos] != '$')
			{
				assert(false);
				addReplyErrorFormat(sendBuf,"Protocol error: expected '$', got '%c'",queryBuf[pos]);
				return REDIS_ERR;
			}


			ok = string2ll(queryBuf + pos +  1,newline - (queryBuf + pos + 1),&ll);
			if(!ok || ll < 0 || ll > 512 * 1024 * 1024)
			{
				assert(false);
				addReplyError(sendBuf,"Protocol error: invalid bulk length");
				return REDIS_ERR;
			}

			pos += newline - (queryBuf + pos) + 2;
			if(ll >= REDIS_MBULK_BIG_ARG)
			{
				assert(false);
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
			if(argc++ == 0)
			{
				commond.append(queryBuf + pos,bulklen);
			}
			else
			{
				rObj * obj = (rObj*)createStringObject(queryBuf + pos,bulklen);
				obj->calHash();
				robjs.push_back(obj);
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



