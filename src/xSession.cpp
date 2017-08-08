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


	if( (conn->host == redis->masterHost) && (conn->port == redis->masterPort) )
	{
		sendBuf.retrieveAll();
		sendSlaveBuf.retrieveAll();
		sendPubSub.retrieveAll();
	}
	else
	{
		if(sendBuf.readableBytes() > 0 )
		{
			conn->send(&sendBuf);
			sendBuf.retrieveAll();
		}

		if(sendPubSub.readableBytes() > 0 )
		{
			for(auto iter = pubSubTcpconn.begin(); iter != pubSubTcpconn.end(); iter ++)
			{
				(*iter)->send(&sendPubSub);
			}

			pubSubTcpconn.clear();
			sendPubSub.retrieveAll();
		}

		if(redis->repliEnabled)
		{
			MutexLockGuard mu(redis->slaveMutex);
			for(auto it = redis->salvetcpconnMaps.begin(); it != redis->salvetcpconnMaps.end(); it++)
			{
				if(sendSlaveBuf.readableBytes() > 0 )
				{
					it->second->send(&sendSlaveBuf);
				}
			}
			sendSlaveBuf.retrieveAll();
		}
	}
}



bool xSession::checkCommond(rObj*  robjs)
{
	auto it = redis->unorderedmapCommonds.find(robjs);
	if(it == redis->unorderedmapCommonds.end())
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

	if (redis->clusterEnabled && redis->clusterSlotEnabled)
	{
		if (!(strcasecmp(robjs[0]->ptr, "cluster")) || !(strcasecmp(robjs[0]->ptr, "migrate")))
		{

		}
		else
		{
			bool mark = false;
			if (robjs.size() >= 2)
			{
				std::string ipPort = conn->host + "::" + std::to_string(conn->port);
				
				const char * key = robjs[1]->ptr;
				int hashslot = redis->clus.keyHashSlot((char*)key, sdsllen(key));
				
				MutexLockGuard mu(redis->clusterMutex);
				if(redis->clusterRepliEnabled)
				{
					
					for(auto it = redis->clus.migratingSlosTos.begin(); it != redis->clus.migratingSlosTos.end(); it ++)
					{
						auto iter = it->second.find(hashslot);
						if(iter != it->second.end())
						{
							mark = true;
							break;
						}
					}
				}

				if(mark)
				{
					redis->structureRedisProtocol(redis->clusterCached,robjs);
				}

				{
					{
						auto it = redis->clus.importingSlotsFrom.find(ipPort);
						if(it !=  redis->clus.importingSlotsFrom.end())
						{
							auto iter = it->second.find(hashslot);
							if(iter != it->second.end())
							{
								goto err;
							}
						}
					}
				}

				
				
				if (redis->clus.clusterSlotNodes.size() == 0)
				{
					redis->clus.clusterRedirectClient(this, nullptr, hashslot, CLUSTER_REDIR_DOWN_UNBOUND);
					clearObj();
					return REDIS_ERR;
				}

				auto it = redis->clus.clusterSlotNodes.find(hashslot);
				if (it == redis->clus.clusterSlotNodes.end())
				{
					redis->clus.clusterRedirectClient(this, nullptr, hashslot, CLUSTER_REDIR_DOWN_UNBOUND);
					clearObj();
					return REDIS_ERR;
				}
				else
				{
					if (redis->host == it->second.ip && redis->port == it->second.port)
					{

					}
					else
					{
						redis->clus.clusterRedirectClient(this, &(it->second), hashslot, CLUSTER_REDIR_MOVED);
						clearObj();
						return REDIS_ERR;
					}
				}
			}

		}
	}

err:
		
	bool fromSalve = false;
	
	if(redis->repliEnabled)
	{
		{
			MutexLockGuard mu(redis->slaveMutex);
			auto it = redis->salvetcpconnMaps.find(conn->getSockfd());
			if(it !=  redis->salvetcpconnMaps.end())
			{
				fromSalve = true;
				if(memcmp(robjs[0]->ptr,shared.ok->ptr,3) == 0)
				{
					if(++redis->salveCount >= redis->salvetcpconnMaps.size())
					{
						LOG_INFO<<"slaveof sync success";
						if(redis->slaveCached.readableBytes() > 0)
						{
							conn->send(&redis->slaveCached);
							redis->slaveCached.retrieveAll();
							xBuffer buffer;
							redis->slaveCached.swap(buffer);
						}
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
					clearObj();
					return REDIS_OK;
				}
			}
			
		}

		if( (conn->host == redis->masterHost) && (conn->port == redis->masterPort) )
		{

		}
		else
		{
			if( redis->masterPort == 0 )
			{
				if(!fromSalve)
				{
					MutexLockGuard mu(redis->slaveMutex);
					if(redis->salveCount <  redis->salvetcpconnMaps.size())
					{
						redis->structureRedisProtocol(redis->slaveCached,robjs);
					}
					else
					{
						redis->structureRedisProtocol(sendSlaveBuf,robjs);
					}
				}
				

			}
			else
			{
				if(!checkCommond(robjs[0]))
				{
					if(redis->slaveEnabled)
					{
						clearObj();
						addReplyErrorFormat(sendBuf,"slaveof mode commond unknown ");
						return REDIS_ERR;
					}
				}
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

	rObj * obj = robjs.front();
	robjs.pop_front();
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
		sdsfree(argv[j]);
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

				addReplyErrorFormat(sendBuf,"Protocol error: expected '$', got '%c'",queryBuf[pos]);
				LOG_INFO<<"Protocol error: &";
				return REDIS_ERR;
			}


			ok = string2ll(queryBuf + pos +  1,newline - (queryBuf + pos + 1),&ll);
			if(!ok || ll < 0 || ll > 512 * 1024 * 1024)
			{

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



