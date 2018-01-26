#include "xRedLock.h"



/* Turn the plain C strings into Sds strings */
static char **convertToSds(int count, char** args)
{
	int j;
	char **sds = (char**)zmalloc(sizeof(char*)*count);
	for(j = 0; j < count; j++)
		sds[j] = sdsnew(args[j]);
	return sds;
}

xLock::xLock()
:validityTime(0),
 resource(nullptr),
 val(nullptr)
{

}

xLock::~xLock()
{
	sdsfree(resource);
	sdsfree(val);
}


int xRedLock::defaultRetryCount  = 3;
int xRedLock::defaultRetryDelay = 100;
float xRedLock::clockDriftFactor = 0.01;

xRedLock::xRedLock()
{
	continueLockScript = sdsnew("if redis.call('get', KEYS[1]) == ARGV[1] then redis.call('del', KEYS[1]) end return redis.call('set', KEYS[1], ARGV[2], 'px', ARGV[3], 'nx')");
	unlockScript  = sdsnew("if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end");
	retryCount = defaultRetryCount;
	retryDelay = defaultRetryDelay;
	quoRum     = 0;
	// open rand file
	fd = open("/dev/urandom", O_RDONLY);
	if (fd == -1)
	{
		//Open error handling
		printf("Can't open file /dev/urandom\n");
		exit(-1);
	}


}

xRedLock::~xRedLock()
{
	sdsfree(continueLockScript);
	sdsfree(unlockScript);
	::close(fd);

}

sds	 xRedLock::getUniqueLockId()
{
	unsigned char buffer[20];
	if(::read(fd,buffer,sizeof(buffer)) == sizeof(buffer))
	{
		sds s;
		s = sdsempty();
		for (int i = 0; i < 20; i++)
		{
			s = sdscatprintf(s, "%02X", buffer[i]);
		}
		return s;
	}
	else
	{
		LOG_WARN<<"Error: GetUniqueLockId";
	}

	return nullptr;
}



int  xRedLock::lockInstance(const xRedisContextPtr &c,const char * resource,const  char *val,const int ttl)
{

	redisReply *reply;
	reply = (redisReply *)redisCommand(c, "set %s %s px %d nx",resource, val, ttl);

	if (reply)
	{
		LOG_INFO<<"Set return "<<reply->str<<" [null == fail, OK == success]";
	}

	if (reply && reply->str && strcmp(reply->str, "OK") == 0)
	{
		freeReply(reply);
		return 1;
	}

	if (reply)
	{
		freeReply(reply);
		return 0;
	}

	LOG_WARN<<"redis disconnect err:"<<c->errstr;
	return -1;

}


redisReply * xRedLock::commandArgv(const xRedisContextPtr &c, int argc, char **inargv)
{
	char **argv = convertToSds(argc, inargv);
	/* Setup argument length */
	size_t *argvlen;
	argvlen = (size_t *)zmalloc(argc * sizeof(size_t));
	for (int j = 0; j < argc; j++)
	{
		argvlen[j] = sdslen(argv[j]);
	}

	redisReply *reply = nullptr;
	reply = (redisReply *)redisCommandArgv(c, argc, (const char **)argv, argvlen);
	if (reply)
	{
		LOG_INFO<<"redisCommandArgv return "<< reply->integer;
	}

	zfree(argvlen);
	sdsfreesplitres(argv, argc);
	return reply;
}

void  xRedLock::unlockInstance(const xRedisContextPtr &c,const char * resource,const  char *val)
{
	int argc = 5;
	char *unlockScriptArgv[] = {(char*)"EVAL",
								unlockScript,
								(char*)"1",
								(char*)resource,
								(char*)val};
	redisReply *reply = commandArgv(c, argc, unlockScriptArgv);
	if (reply)
	{
		freeReply(reply);
	}

}


bool xRedLock::unlock(const xLock &lock)
{
	for(auto it = syncServerMaps.begin(); it != syncServerMaps.end(); ++it)
	{
		unlockInstance(it->second,lock.resource,lock.val);
	}

	return true;
}

bool xRedLock::lock(const char *resource, const int ttl, xLock &lock)
{
	sds val = getUniqueLockId();
	if (!val)
	{
		return false;
	}

	lock.resource = sdsnew(resource);
	lock.val = val;

	LOG_INFO<<"Get the unique id is" <<val;

	int count = retryCount;
	int n = 0;
	do
	{
		int startTime = (int)time(nullptr) * 1000;

		for(auto it = syncServerMaps.begin(); it != syncServerMaps.end();)
		{
			int r = lockInstance(it->second,resource,val,ttl);
			if(r == 1)
			{
				n++;
				++it;
			}
			else if(r ==-1)
			{
				LOG_INFO<<"reconnect redis..................";
				auto c = redisConnectWithTimeout(it->second->addr, it->second->port, timeout);
				if (c == nullptr || c->err)
				{
					if (c)
					{
						LOG_WARN<<"Connection error "<<c->errstr;
					}
					else
					{
						LOG_WARN<<"Connection error: can't allocate redis context";
					}
					++it;
				}
				else
				{
					LOG_INFO<<"reconnect redis success..................";
					syncServerMaps.erase(it++);
					disconnectServers.push_back(c);
				}
			}
			else
			{
				++it;
			}

		}

		int drift = (ttl * clockDriftFactor) + 2;
		int validityTime = ttl - ((int)time(nullptr) * 1000 - startTime) - drift;
		LOG_INFO<<"The resource validty time is "<< validityTime<<" is "<<quoRum;
		if (n >=  quoRum && validityTime > 0)
		{
			lock.validityTime = validityTime;
			for(auto it = disconnectServers.begin(); it  !=disconnectServers.end(); ++it)
			{
				syncServerMaps.insert(std::make_pair((*it)->fd,*it));
			}

			disconnectServers.clear();

			return true;
		}
		else
		{
			unlock(lock);
		}

		int delay = rand() % retryDelay + retryDelay / 2;
		usleep(delay * 1000);
		count--;

	}while(count > 0);

	return false;
}


void  xRedLock::syncAddServerUrl(const char *ip,const int port)
{
	xRedisContextPtr c;
	redisReply *reply;
	c = redisConnectWithTimeout(ip, port, timeout);
	if (c == nullptr || c->err)
	{
		if (c)
		{
			LOG_WARN<<"Connection error "<<c->errstr;
		}
		else
		{
			LOG_WARN<<"Connection error: can't allocate redis context";
		}
		exit(1);
	}

	syncServerMaps.insert(std::make_pair(c->fd,c));
	quoRum = syncServerMaps.size()/ 2 + 1;
}


void xRedLock::asyncAddServerUrl(const char *ip,const int port)
{

}

