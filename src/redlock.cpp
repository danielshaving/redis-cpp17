#include "redlock.h"

static char **convertToSds(int32_t count, char **args)
{
	int32_t j;
	char **sds = (char**)zmalloc(sizeof(char*)*count);
	for(j = 0; j < count; j++)
		sds[j] = sdsnew(args[j]);
	return sds;
}

Lock::Lock()
:validityTime(0),
 resource(nullptr),
 val(nullptr)
{

}

Lock::~Lock()
{
	sdsfree(resource);
	sdsfree(val);
}

int32_t RedLock::defaultRetryCount = 3;
int32_t RedLock::defaultRetryDelay = 100;
float RedLock::clockDriftFactor = 0.01;

RedLock::RedLock()
{
	timeout = { 1,500000 };
	continueLockScript = sdsnew("if redis.call('get', KEYS[1]) == \
			ARGV[1] then redis.call('del', KEYS[1]) end return \
			redis.call('set', KEYS[1], ARGV[2], 'px', ARGV[3], 'nx')");
	unlockScript = sdsnew("if redis.call('get', KEYS[1]) == ARGV[1] \
			 then return redis.call('del', KEYS[1]) else return 0 end");
	retryCount = defaultRetryCount;
	retryDelay = defaultRetryDelay;
	quoRum = 0;
	fd = open("/dev/urandom",O_RDONLY);
	if (fd == -1)
	{
		printf("Can't open file /dev/urandom\n");
		exit(-1);
	}
}

RedLock::~RedLock()
{
	sdsfree(continueLockScript);
	sdsfree(unlockScript);
	::close(fd);
}

sds RedLock::getUniqueLockId()
{
	unsigned char buffer[20];
	if(::read(fd,buffer,sizeof(buffer)) == sizeof(buffer))
	{
		sds s;
		s = sdsempty();
		for (int32_t i = 0; i < 20; i++)
		{
			s = sdscatprintf(s,"%02X",buffer[i]);
		}
		return s;
	}
	else
	{
		LOG_WARN<<"Error: GetUniqueLockId";
	}
	return nullptr;
}

int32_t RedLock::lockInstance(const RedisContextPtr &c,
		const char *resource,const char *val,const int32_t ttl)
{
	RedisReply *reply;
	reply = (RedisReply *)c->redisCommand("set %s %s px %d nx",resource,val,ttl);

	if (reply)
	{
		LOG_INFO<<"Set return "<<reply->str<<" [null == fail, OK == success]";
	}

	if (reply && reply->str && strcmp(reply->str,"OK") == 0)
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

RedisReply *RedLock::commandArgv(const RedisContextPtr &c,int32_t argc,char **inargv)
{
	char **argv = convertToSds(argc, inargv);
	size_t *argvlen;
	argvlen = (size_t *)zmalloc(argc * sizeof(size_t));
	for (int32_t j = 0; j < argc; j++)
	{
		argvlen[j] = sdslen(argv[j]);
	}

	RedisReply *reply = nullptr;
	reply = (RedisReply *)c->redisCommandArgv(argc,(const char **)argv,argvlen);
	if (reply)
	{
		LOG_INFO<<"redisCommandArgv return "<< reply->integer;
	}

	zfree(argvlen);
	sdsfreesplitres(argv, argc);
	return reply;
}

void RedLock::unlockInstance(const RedisContextPtr &c,const char *resource,const char *val)
{
	int32_t argc = 5;
	char *unlockScriptArgv[] = {(char*)"EVAL",
								unlockScript,
								(char*)"1",
								(char*)resource,
								(char*)val};
	RedisReply *reply = commandArgv(c,argc,unlockScriptArgv);
	if (reply)
	{
		freeReply(reply);
	}
}

bool RedLock::unlock(const Lock &lock)
{
	for (auto &it : syncServers)
	{
		unlockInstance(it.second,lock.resource,lock.val);
	}
	return true;
}

bool RedLock::lock(const char *resource,const int32_t ttl,Lock &lock)
{
	sds val = getUniqueLockId();
	if (!val)
	{
		return false;
	}

	lock.resource = sdsnew(resource);
	lock.val = val;

	LOG_INFO<<"Get the unique id is" <<val;

	int32_t count = retryCount;
	int32_t n = 0;
	do
	{
		int32_t startTime = (int32_t)time(nullptr) * 1000;

		for(auto it = syncServers.begin(); it != syncServers.end();)
		{
			int32_t r = lockInstance(it->second,resource,val,ttl);
			if(r == 1)
			{
				n++;
				++it;
			}
			else if(r == -1)
			{
				LOG_INFO<<"reconnect redis..................";
				auto c = redisConnectWithTimeout(it->second->ip,it->second->port,timeout);
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
					syncServers.erase(it++);
					disconnectServers.push_back(c);
				}
			}
			else
			{
				++it;
			}
		}

		int32_t drift = (ttl * clockDriftFactor) + 2;
		int32_t validityTime = ttl - ((int32_t)time(nullptr) * 1000 - startTime) - drift;
		LOG_INFO<<"The resource validty time is "<< validityTime<<" is "<<quoRum;
		if (n >=  quoRum && validityTime > 0)
		{
			lock.validityTime = validityTime;
			for(auto &it : disconnectServers)
			{
				syncServers.insert(std::make_pair(it->fd,it));
			}
			disconnectServers.clear();
			return true;
		}
		else
		{
			unlock(lock);
		}

		int32_t delay = rand() % retryDelay + retryDelay / 2;
		usleep(delay * 1000);
		count--;

	}while(count > 0);
	return false;
}

void RedLock::syncAddServerUrl(const char *ip,const int16_t port)
{
	RedisContextPtr c;
	RedisReply *reply;
	c = redisConnectWithTimeout(ip,port,timeout);
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

	syncServers.insert(std::make_pair(c->fd,c));
	quoRum = syncServers.size()/ 2 + 1;
}


void RedLock::asyncAddServerUrl(const char *ip,const int16_t port)
{

}

