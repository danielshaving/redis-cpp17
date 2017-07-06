#include "xSession.h"
#include "xRedis.h"

xSession::xSession(xRedis *redis,const xTcpconnectionPtr & conn)
:reqtype(0),
 multibulklen(0),
 bulklen(-1),
 argc(0),
 conn(conn),
 redis(redis),
 authEnabled(false)
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


	if(sendBuf.readableBytes() > 0 )
	{
		conn->send(&sendBuf);
	}


//	for(auto it = redis->tcpconnMaps.begin(); it != redis->tcpconnMaps.end(); it++)
//	{
//		if(sendSlaveBuf.readableBytes() > 0 )
//		{
//			it->second->send(&sendSlaveBuf);
//		}
//	}


}


bool xSession::checkCommond(rObj*  robjs,int size)
{
	auto it = redis->unorderedmapCommonds.find(robjs);
	if(it == redis->unorderedmapCommonds.end())
	{
		return true;
	}

	if(it->second !=  size)
	{
		return true;
	}
		
	return false;
}


int xSession::processCommand()
{
	assert(robjs.size());
	if(redis->authEnabled)
	{
		if(!authEnabled)
		{
			if (strcasecmp(robjs[0]->ptr,"auth") != 0)
			{
				clearObj();
				addReplyErrorFormat(sendBuf,"NOAUTH Authentication required");
				return REDIS_ERR;
			}
		}
	}

	{
		MutexLockGuard mu(redis->slaveMutex);
		auto it = redis->tcpconnMaps.find(conn->getSockfd());
		if(it !=  redis->tcpconnMaps.end())
		{
			if(memcmp(robjs[0]->ptr,shared.ok->ptr,3) == 0)
			{
				LOG_INFO<<"slaveof sync success";
				if(redis->slaveCached.readableBytes() > 0)
				{
					conn->send(&redis->slaveCached);
					redis->tcpconnMaps.erase(conn->getSockfd());
				}

				auto iter = redis->repliTimers.find(conn->getSockfd());
				if(iter == redis->repliTimers.end())
				{
					assert(false);
				}

				if(iter->second)
				{
					conn->getLoop()->cancelAfter(iter->second);
				}

				redis->repliTimers.erase(conn->getSockfd());

				if(redis->tcpconnMaps.size() == 0)
				{
					xBuffer buffer;
					buffer.swap(redis->slaveCached);
					LOG_INFO<<"swap buffer";
				}

				clearObj();
				return REDIS_OK;
			}
			else
			{
				replicationFeedSlaves(redis->slaveCached,robjs[0],robjs);
			}
		}
	}

	auto iter = redis->handlerCommondMap.find(robjs[0]);
	if(iter == redis->handlerCommondMap.end() )
	{
		clearObj();
		addReplyErrorFormat(sendBuf,"command unknown eror");
		return REDIS_ERR;
	}

	rObj * obj = robjs[0];
	robjs.pop_front();
	bool flag = checkCommond(obj,robjs.size());
	if(!flag)
	{
		for(auto it = redis->tcpconnMaps.begin(); it != redis->tcpconnMaps.end(); it++)
		{
			replicationFeedSlaves(sendSlaveBuf,obj,robjs);
		}

		if(  (conn->host == redis->masterHost) && (conn->port == redis->masterPort) )
		{

		}
		else
		{
			if(redis->slaveEnabled)
			{
				clearObj();
				addReplyErrorFormat(sendBuf,"slaveof mode commond unknown ");
				return REDIS_ERR;
			}
		}
	}
	

	zfree(obj);
	if(!iter->second(robjs,this))
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
	for(auto it = robjs.begin(); it != robjs.end(); it++)
	{
		zfree(*it);
	}	
}

void xSession::reset()
{
	 argc = 0;
	 multibulklen = 0;
	 bulklen = -1;
	 robjs.clear();
}

int xSession::processInlineBuffer(xBuffer *recvBuf)
{
    const char *newline;
    const char *queryBuf = recvBuf->peek();
    int  j;
    size_t queryLen;
    sds *argv, aux;
	  
    /* Search for end of line */
    newline = strchr(queryBuf,'\n');
    if(newline == nullptr)
    {
    	 if(recvBuf->readableBytes() > PROTO_INLINE_MAX_SIZE)
    	 {
    	 	  addReplyError(sendBuf,"Protocol error: too big inline request");
	        return REDIS_ERR;
    	 }
		 
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
		addReplyError(sendBuf,"Protocol error: unbalanced quotes in request");
		return REDIS_ERR;
       }

	for (j = 0; j  < argc; j++)
	{
		rObj * obj = (rObj*)createStringObject(argv[j],sdslen(argv[j]));
		robjs.push_back(obj);
	}
	
	zfree(argv);
	return REDIS_OK;
   
}

int xSession::processMultibulkBuffer(xBuffer *recvBuf)
{
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
				addReplyError(sendBuf,"Protocol error: too big mbulk count string");
				LOG_INFO<<"Protocol error: too big mbulk count string";
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
			return REDIS_ERR;
		}

		ok = string2ll(queryBuf + 1,newline - ( queryBuf + 1),&ll);
		if(!ok || ll > 1024 * 1024)
		{
			addReplyError(sendBuf,"Protocol error: invalid multibulk length");
			LOG_INFO<<"Protocol error: invalid multibulk length";
			assert(false);
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
					assert(false);
					addReplyError(sendBuf,"Protocol error: too big bulk count string");
					LOG_INFO<<"Protocol error: too big bulk count string";
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
				assert(false);
				addReplyErrorFormat(sendBuf,"Protocol error: expected '$', got '%c'",queryBuf[pos]);
				LOG_INFO<<"Protocol error: &";
				return REDIS_ERR;
			}


			ok = string2ll(queryBuf + pos +  1,newline - (queryBuf + pos + 1),&ll);
			if(!ok || ll < 0 || ll > 512 * 1024 * 1024)
			{
				assert(false);
				addReplyError(sendBuf,"Protocol error: invalid bulk length");
				LOG_INFO<<"Protocol error: invalid bulk length";
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
			rObj * obj = (rObj*)createStringObject(queryBuf + pos,bulklen);
			robjs.push_back(obj);
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



