#include "xSession.h"
#include "xRedis.h"

xSession::xSession(xRedis *redis,const xTcpconnectionPtr & conn)
:reqtype(0),
 multibulklen(0),
 bulklen(-1),
 argc(0),
 conn(conn),
 redis(redis),
 authEnabled(false),
 retrieveBuffer(false),
 fromMaster(false),
 fromSlave(false)
{
	command = createStringObject(nullptr,10);
	conn->setMessageCallback(
	        std::bind(&xSession::readCallBack, this, std::placeholders::_1,std::placeholders::_2,std::placeholders::_3));
}

xSession::~xSession()
{
	zfree(command);
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


	if(fromMaster)
	{
		sendBuf.retrieveAll();
		sendSlaveBuf.retrieveAll();
		sendPubSub.retrieveAll();
		fromMaster = false;
	}
	else
	{
	
		if(sendBuf.readableBytes() > 0 )
		{
			conn->send(&sendBuf);
			sendBuf.retrieveAll();
		}

		if(retrieveBuffer)
		{
			sendBuf.retrieveAll();
			retrieveBuffer = false;
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
			std::unique_lock <std::mutex> lck(redis->slaveMutex);
			for(auto it = redis->salvetcpconnMaps.begin(); it != redis->salvetcpconnMaps.end(); ++it)
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



bool xSession::checkCommand(rObj*  robjs)
{
	auto it = redis->unorderedmapCommands.find(robjs);
	if(it == redis->unorderedmapCommands.end())
	{
		return false;
	}
		
	return true;
}


int xSession::processCommand()
{
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
		{
			auto it = redis->cluterMaps.find(command);
			if(it != redis->cluterMaps.end())
			{
				goto jump;
			}
		}
		
		char * key = robjs[0]->ptr;
		int hashslot = redis->clus.keyHashSlot((char*)key, sdslen(key));
		
		std::unique_lock <std::mutex> lck(redis->clusterMutex);
		if(redis->clusterRepliMigratEnabled)
		{
			for(auto it = redis->clus.migratingSlosTos.begin(); it != redis->clus.migratingSlosTos.end(); ++it)
			{
				auto iter = it->second.find(hashslot);
				if(iter != it->second.end())
				{
					redis->structureRedisProtocol(redis->clusterMigratCached,robjs);
					goto jump;
				}
			}
		}

		

		if(redis->clusterRepliImportEnabeld)
		{
			for(auto it = redis->clus.importingSlotsFrom.begin(); it != redis->clus.importingSlotsFrom.end(); ++it)
			{
				auto iter = it->second.find(hashslot);
				if(iter !=  it->second.end())
				{
					retrieveBuffer = true;
					goto jump;
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
				//FIXME
			}
			else
			{
				redis->clus.clusterRedirectClient(this, &(it->second), hashslot, CLUSTER_REDIR_MOVED);
				clearObj();
				return REDIS_ERR;
			}
	
		}

		
	}
	

jump:
		

	if(redis->repliEnabled)
	{
		{
			std::unique_lock <std::mutex> lck(redis->slaveMutex);
			auto it = redis->salvetcpconnMaps.find(conn->getSockfd());
			if(it !=  redis->salvetcpconnMaps.end())
			{
				if(memcmp(command->ptr,shared.ok->ptr,sdslen(command->ptr)) == 0)
				{
					if(++redis->salveCount >= redis->salvetcpconnMaps.size())
					{
						LOG_INFO<<"slaveof sync success";
						if(redis->slaveCached.readableBytes() > 0)
						{
							conn->send(&redis->slaveCached);
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
					return REDIS_OK;
				}
			}
			
		}

		if( (conn->host == redis->masterHost) && (conn->port == redis->masterPort) )
		{
			fromMaster = true;
			if(!checkCommand(command))
			{
				clearObj();
				LOG_WARN<<"master sync command unknow " << command;
				return REDIS_ERR;
			}
		}
		else if( redis->masterHost.length() > 0 && redis->masterPort > 0 )
		{
			if(checkCommand(command))
			{
				clearObj();
				addReplyErrorFormat(sendBuf,"slaveof command unknown");
				return REDIS_ERR;
			}
		}
 		else
		{
			if(checkCommand(command))
			{
				robjs.push_front(command);
				{
					std::unique_lock <std::mutex> lck(redis->slaveMutex);
					if(redis->salveCount <  redis->salvetcpconnMaps.size())
					{
						redis->structureRedisProtocol(redis->slaveCached,robjs);
					}
					else
					{
						redis->structureRedisProtocol(sendSlaveBuf,robjs);
					}
					robjs.pop_front();
				}
			}
		}

	}



	auto iter = redis->handlerCommandMap.find(command);
	if(iter == redis->handlerCommandMap.end() )
	{
		clearObj();
		addReplyErrorFormat(sendBuf,"command unknown eror");
		return REDIS_ERR;
	}

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
	for(auto it = robjs.begin(); it != robjs.end(); ++it)
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
			LOG_WARN << "Protocol error";
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
		LOG_WARN << "Protocol error";
		addReplyError(sendBuf,"Protocol error: unbalanced quotes in request");
		return REDIS_ERR;
       }

	for (j = 0; j  < argc; j++)
	{
		if(j == 0)
		{
			sdscpylen((sds)(command->ptr),argv[j],sdslen(argv[j]));
		}
		else
		{
			rObj * obj = (rObj*)createStringObject(argv[j],sdslen(argv[j]));
					robjs.push_back(obj);
		}
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
				LOG_WARN << "Protocol error";
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
			if(++argc == 1)
			{
				sdscpylen(command->ptr,queryBuf + pos,bulklen);
			}
			else
			{
				rObj * obj = (rObj*)createStringObject((char*)(queryBuf + pos),bulklen);
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



