#include "xObject.h"
#include "xRedis.h"


rObj *xObjects::createObject(int32_t type,void *ptr)
{
	rObj *o = (rObj*)zmalloc(sizeof(rObj));
	o->encoding = REDIS_ENCODING_RAW;
	o->type = type;
	o->ptr  = (char*)ptr;
	return o;
}


int32_t xObjects::getLongLongFromObject(rObj *o,int64_t *target)
{
	int64_t value;

	if (o == nullptr)
	{
		value = 0;
	}
	else
	{
		if (sdsEncodedObject(o))
		{
			if (string2ll(o->ptr,sdslen(o->ptr),&value) == 0) return REDIS_ERR;
		}
		else
		{
			LOG_WARN << "Unknown string encoding";
		}
	}
	
	if (target) *target = value;
	return REDIS_OK;
}

int32_t xObjects::getLongLongFromObjectOrReply(xBuffer &buffer,rObj *o,int64_t *target,const char *msg)
{
    int64_t value;
    if (getLongLongFromObject(o,&value) != REDIS_OK)
    {
        if (msg != nullptr)
        {
            addReplyError(buffer,(char*)msg);
        }
        else
        {
            addReplyError(buffer,"value is no an integer or out of range");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}


int xObjects::getLongFromObjectOrReply(xBuffer &buffer,rObj *o,int32_t *target,const char *msg)
{
	int64_t value;
	if (getLongLongFromObject(o,&value) != REDIS_OK)
	{
		if (msg != nullptr) 
		{
			addReplyError(buffer,(char*)msg);
		}
		else 
		{
			addReplyError(buffer, "value is no an integer or out of range");
		}
		return REDIS_ERR;
	}
	
	*target = value;
	return REDIS_OK;
}

rObj *xObjects::createStringObjectFromLongLong(int64_t value)
{
	rObj *o;
	if(value <=0 && value < REDIS_SHARED_INTEGERS)
	{
		o = integers[value];
	}
	else
	{
		if(value >= LONG_MIN && value <= LONG_MAX)
		{
			o = createObject(REDIS_STRING, nullptr);
			o->encoding = REDIS_ENCODING_INT;
			o->ptr = (char*)value;
		}
		else
		{
			o = createObject(REDIS_STRING,sdsfromlonglong(value));
		}
	}
	return o;
}

int32_t xObjects::getDoubleFromObjectOrReply(xBuffer &buffer,rObj *o,double *target,const char *msg)
{
    double value;
    if (getDoubleFromObject(o,&value) != REDIS_OK)
    {
        if (msg != nullptr)
        {
            addReplyError(buffer,(char*)msg);
        }
        else
        {
            addReplyError(buffer,"value is no a valid float");
        }
        return REDIS_ERR;
    }
    *target = value;
    return REDIS_OK;
}


int xObjects::getDoubleFromObject(const rObj *o,double *target)
{
    double value;
    char *eptr;

    if (o == nullptr)
    {
        value = 0;
    }
    else
    {
        if (sdsEncodedObject(o))
        {
            errno = 0;
            value = strtod(o->ptr, &eptr);
            if (isspace(((const char*)o->ptr)[0]) ||
                eptr[0] != '\0' ||
                (errno == ERANGE && value == 0) || errno == EINVAL)
                return REDIS_ERR;
        }
        else if (o->encoding == OBJ_ENCODING_INT)
        {
            value = (long)o->ptr;
        }
        else
        {
            LOG_WARN<<"Unknown string encoding";
        }
    }
    *target = value;
    return REDIS_OK;
}

void xObjects::freeStringObject(rObj *o)
{
    if (o->encoding == OBJ_ENCODING_RAW)
    {
        sdsfree((sds)o->ptr);
    }
}

void xObjects::freeListObject(rObj *o)
{
    if (o->encoding == OBJ_ENCODING_RAW)
    {
        sdsfree((sds)o->ptr);
    }
}

void xObjects::freeHashObject(rObj *o)
{
    if (o->encoding == OBJ_ENCODING_RAW)
    {
        sdsfree((sds)o->ptr);
    }
}

void xObjects::freeSetObject(rObj *o)
{
    if (o->encoding == OBJ_ENCODING_RAW)
    {
        sdsfree((sds)o->ptr);
    }
}

void xObjects::freeZsetObject(rObj *o)
{
    if (o->encoding == OBJ_ENCODING_RAW)
    {
        sdsfree((sds)o->ptr);
    }
}

void xObjects::decrRefCount(rObj *o)
{
	switch(o->type)
	{
        case OBJ_STRING: freeStringObject(o); break;
        case OBJ_LIST: freeListObject(o); break;
        case OBJ_SET: freeSetObject(o); break;
        case OBJ_ZSET: freeZsetObject(o); break;
        case OBJ_HASH: freeHashObject(o); break;
        default: LOG_ERROR<<"Unknown object type"; break;
	}
	zfree(o);
}

xObjects::xObjects(xRedis *redis)
:redis(redis)
{

}

xObjects::~xObjects()
{

}

void xObjects::destorySharedObjects()
{
	freeStringObject(crlf);
	freeStringObject(ok);
	freeStringObject(err);
	freeStringObject(emptybulk);
	freeStringObject(czero);
	freeStringObject(cone);
	freeStringObject(cnegone);
	freeStringObject(nullbulk);
	freeStringObject(nullmultibulk);
	freeStringObject(emptymultibulk);
	freeStringObject(pping);
	freeStringObject(ping);
	freeStringObject(pong);
	freeStringObject(ppong);
	freeStringObject(queued);
	freeStringObject(emptyscan);
	freeStringObject(wrongtypeerr);
	freeStringObject(nokeyerr);
	freeStringObject(syntaxerr);
	freeStringObject(sameobjecterr);
	freeStringObject(outofrangeerr);
	freeStringObject(noscripterr);
	freeStringObject(loadingerr);
	freeStringObject(slowscripterr);
	freeStringObject(masterdownerr);
	freeStringObject(bgsaveerr);
	freeStringObject(roslaveerr);
	freeStringObject(noautherr);
	freeStringObject(oomerr);
	freeStringObject(execaborterr);
	freeStringObject(noreplicaserr);
	freeStringObject(busykeyerr);
	freeStringObject(space);
	freeStringObject(colon);
	freeStringObject(plus);
	freeStringObject(messagebulk);
	freeStringObject(pmessagebulk);
	freeStringObject(subscribebulk);
	freeStringObject(unsubscribebulk);
	freeStringObject(psubscribebulk);
	freeStringObject(punsubscribebulk);
	freeStringObject(del);
	freeStringObject(rpop);
	freeStringObject(lpop);
	freeStringObject(lpush);
	freeStringObject(rpush);
	freeStringObject(set);
	freeStringObject(get);
	freeStringObject(flushdb);
	freeStringObject(dbsize);
	freeStringObject(hset);
	freeStringObject(hget);
	freeStringObject(hgetall);
	freeStringObject(save);
	freeStringObject(slaveof);
	freeStringObject(command);
	freeStringObject(config);
	freeStringObject(auth);
	freeStringObject(info);
	freeStringObject(echo);
	freeStringObject(client);
	freeStringObject(hkeys);
	freeStringObject(hlen);
	freeStringObject(keys);
	freeStringObject(bgsave);
	freeStringObject(memory);
	freeStringObject(cluster);
	freeStringObject(migrate);
	freeStringObject(debug);
	freeStringObject(ttl);
	freeStringObject(lrange);
	freeStringObject(llen);
	freeStringObject(sadd);
	freeStringObject(scard);
	freeStringObject(addsync);
	freeStringObject(setslot);
	freeStringObject(node);
	freeStringObject(connect);
	freeStringObject(delsync);
	freeStringObject(psync);
	freeStringObject(sync);
	freeStringObject(zadd);
	freeStringObject(zrevrange);
	freeStringObject(zcard);
	freeStringObject(dump);
	freeStringObject(restore);
	
	freeStringObject(PING);
	freeStringObject(DEL);
	freeStringObject(RPOP);
	freeStringObject(LPOP);
	freeStringObject(LPUSH);
	freeStringObject(RPUSH);
	freeStringObject(SET);
	freeStringObject(GET);
	freeStringObject(FLUSHDB);
	freeStringObject(DBSIZE);
	freeStringObject(HSET);
	freeStringObject(HGET);
	freeStringObject(HGETALL);
	freeStringObject(SAVE);
	freeStringObject(SLAVEOF);
	freeStringObject(COMMAND);
	freeStringObject(CONFIG);
	freeStringObject(AUTH);
	freeStringObject(INFO);
	freeStringObject(ECHO);
	freeStringObject(CLIENT);
	freeStringObject(HKEYS);
	freeStringObject(HLEN);
	freeStringObject(KEYS);
	freeStringObject(BGSAVE);
	freeStringObject(MEMORY);
	freeStringObject(CLUSTER);
	freeStringObject(MIGRATE);
	freeStringObject(DEBUG);
	freeStringObject(TTL);
	freeStringObject(LRANGE);
	freeStringObject(LLEN);
	freeStringObject(SADD);
	freeStringObject(SCARD);
	freeStringObject(ADDSYNC);
	freeStringObject(SETSLOT);
	freeStringObject(NODE);
	freeStringObject(CONNECT);
	freeStringObject(DELSYNC);
	freeStringObject(PSYNC);
	freeStringObject(SYNC);
	freeStringObject(ZADD);
	freeStringObject(ZREVRANGE);
	freeStringObject(ZCARD);
	freeStringObject(DUMP);
	freeStringObject(RESTORE);
	
	for (int j = 0; j < REDIS_SHARED_BULKHDR_LEN; j++)
	{
		freeStringObject(integers[j]);
	}

	for (int j = 0; j < REDIS_SHARED_INTEGERS; j++)
	{
		freeStringObject(mbulkhdr[j]);
	}

	zfree(rIp);
	zfree(rPort);

}

void xObjects::createSharedObjects()
{
	int j;

	crlf = createObject(REDIS_STRING,sdsnew("\r\n"));
	ok = createObject(REDIS_STRING,sdsnew("+OK\r\n"));
	err = createObject(REDIS_STRING,sdsnew("-ERR\r\n"));
	emptybulk = createObject(REDIS_STRING,sdsnew("$0\r\n\r\n"));
	czero = createObject(REDIS_STRING,sdsnew(":0\r\n"));
	cone = createObject(REDIS_STRING,sdsnew(":1\r\n"));
	cnegone = createObject(REDIS_STRING,sdsnew(":-1\r\n"));
	nullbulk = createObject(REDIS_STRING,sdsnew("$-1\r\n"));
	nullmultibulk = createObject(REDIS_STRING,sdsnew("*-1\r\n"));
	emptymultibulk = createObject(REDIS_STRING,sdsnew("*0\r\n"));
	pping = createObject(REDIS_STRING, sdsnew("PPING\r\n"));
	ping = createObject(REDIS_STRING,sdsnew("PING\r\n"));
	pong = createObject(REDIS_STRING, sdsnew("+PONG\r\n"));
	ppong = createObject(REDIS_STRING,sdsnew("PPONG"));
	queued = createObject(REDIS_STRING,sdsnew("+QUEUED\r\n"));
	emptyscan = createObject(REDIS_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));

	wrongtypeerr = createObject(REDIS_STRING,sdsnew(
	    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
	nokeyerr = createObject(REDIS_STRING,sdsnew(
	    "-ERR no such key\r\n"));
	syntaxerr = createObject(REDIS_STRING,sdsnew(
	    "-ERR syntax error\r\n"));
	sameobjecterr = createObject(REDIS_STRING,sdsnew(
	    "-ERR source and destination objects are the same\r\n"));
	outofrangeerr = createObject(REDIS_STRING,sdsnew(
	    "-ERR index out of range\r\n"));
	noscripterr = createObject(REDIS_STRING,sdsnew(
	    "-NOSCRIPT No matching script. Please use EVAL.\r\n"));
	loadingerr = createObject(REDIS_STRING,sdsnew(
	    "-LOADING Redis is loading the dataset in memory\r\n"));
	slowscripterr = createObject(REDIS_STRING,sdsnew(
	    "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
	masterdownerr = createObject(REDIS_STRING,sdsnew(
	    "-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n"));
	bgsaveerr = createObject(REDIS_STRING,sdsnew(
	    "-MISCONF Redis is configured to save RDB snapshots, but is currently no able to persist on disk. Commands that may modify the data set are disabled. Please check Redis logs for details about the error.\r\n"));
	roslaveerr = createObject(REDIS_STRING,sdsnew(
	    "-READONLY You can't write against a read only slave.\r\n"));
	noautherr = createObject(REDIS_STRING,sdsnew(
	    "-NOAUTH Authentication required.\r\n"));
	oomerr = createObject(REDIS_STRING,sdsnew(
	    "-OOM command no allowed when used memory > 'maxmemory'.\r\n"));
	execaborterr = createObject(REDIS_STRING,sdsnew(
	    "-EXECABORT Transaction discarded because of previous errors.\r\n"));
	noreplicaserr = createObject(REDIS_STRING,sdsnew(
	    "-NOREPLICAS Not enough good slaves to write.\r\n"));
	busykeyerr = createObject(REDIS_STRING,sdsnew(
	    "-BUSYKEY Target key name already exists.\r\n"));

	space = createObject(REDIS_STRING,sdsnew(" "));
	colon = createObject(REDIS_STRING,sdsnew(":"));
	plus = createObject(REDIS_STRING,sdsnew("+"));
	asking = createObject(REDIS_STRING,sdsnew("asking"));

	messagebulk = createStringObject("$7\r\nmessage\r\n",13);
	pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
	subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
	unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
	psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
	punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);

	del = createStringObject("del", 3);
	rpop = createStringObject("rpop", 4);
	lpop = createStringObject("lpop", 4);
	lpush = createStringObject("lpush", 5);
	rpush = createStringObject("rpush", 5);
	set = createStringObject("set", 3);
	get = createStringObject("get", 3);
	flushdb = createStringObject("flushdb", 7);
	dbsize = createStringObject("dbsize", 6);
	hset = createStringObject("hset", 4);
	hget = createStringObject("hget", 4);
	hgetall = createStringObject("hgetall", 7);
	save = createStringObject("save", 4);
	slaveof = createStringObject("slaveof", 7);
	command = createStringObject("command", 7);
	config = createStringObject("config", 6);
	auth = createStringObject("rpush", 5);
	info = createStringObject("info", 4);
	echo = createStringObject("echo", 4);
	client = createStringObject("client", 6);
	hkeys = createStringObject("hkeys", 5);
	hlen = createStringObject("hlen", 4);
	keys = createStringObject("keys", 4);
	bgsave = createStringObject("bgsave", 6);
	memory = createStringObject("memory", 6);
	cluster = createStringObject("cluster", 7);
	migrate = createStringObject("migrate", 7);
	debug = createStringObject("debug", 5);
	ttl = createStringObject("ttl", 3);
	lrange = createStringObject("lrange", 6);
	llen = createStringObject("llen", 4);
	sadd = createStringObject("sadd", 4);
	scard = createStringObject("scard", 5);
	addsync = createStringObject("addsync", 7);
	setslot = createStringObject("setslot", 7);
	node = createStringObject("node", 4);
	connect = createStringObject("connect", 7);
	psync = createStringObject("psync", 5);
	sync = createStringObject("sync", 4);
	delsync = createStringObject("delsync", 7);
	zadd = createStringObject("zadd", 4);
	zrange = createStringObject("zrange",6);
	zrevrange = createStringObject("zrevrange",9);
	zcard = createStringObject("zcard",5);
	dump = createStringObject("dump",4);
	restore =  createStringObject("restore",7);
	
	PING =  createStringObject("PING", 4);
	DEL = createStringObject("DEL", 3);
	RPOP = createStringObject("RPOP", 4);
	LPOP = createStringObject("LPOP", 4);
	LPUSH = createStringObject("LPUSH", 5);
	RPUSH = createStringObject("RPUSH", 5);
	SET = createStringObject("SET", 3);
	GET = createStringObject("GET", 3);
	FLUSHDB = createStringObject("FLUSHDB", 7);
	DBSIZE = createStringObject("DBSIZE", 6);
	HSET = createStringObject("HSET", 4);
	HGET = createStringObject("HGET", 4);
	HGETALL = createStringObject("HGETALL", 7);
	SAVE = createStringObject("SAVE", 4);
	SLAVEOF = createStringObject("SLAVEOF", 7);
	COMMAND = createStringObject("COMMAND", 7);
	CONFIG = createStringObject("CONFIG", 6);
	AUTH = createStringObject("RPUSH", 5);
	INFO = createStringObject("INFO", 4);
	ECHO = createStringObject("ECHO", 4);
	CLIENT = createStringObject("CLIENT", 6);
	HKEYS = createStringObject("HKEYS", 5);
	HLEN = createStringObject("HLEN", 4);
	KEYS = createStringObject("KEYS", 4);
	BGSAVE = createStringObject("BGSAVE", 6);
	MEMORY = createStringObject("MEMORY", 6);
	CLUSTER = createStringObject("CLUSTER", 7);
	MIGRATE = createStringObject("MIGRATE", 7);
	DEBUG = createStringObject("DEBUG", 5);
	TTL = createStringObject("TTL", 3);
	LRANGE = createStringObject("LRANGE", 6);
	LLEN = createStringObject("LLEN", 4);
	SADD = createStringObject("SADD", 4);
	SCARD = createStringObject("SCARD", 5);
	ADDSYNC = createStringObject("ADDSYNC", 7);
	SETSLOT = createStringObject("SETSLOT", 7);
	NODE = createStringObject("NODE", 4);
	CONNECT = createStringObject("CONNECT", 7);
	PSYNC = createStringObject("PSYNC", 5);
	SYNC = createStringObject("SYNC", 4);
	DELSYNC = createStringObject("DELSYNC", 7);
	ZADD = createStringObject("ZADD", 4);
	ZRANGE = createStringObject("ZRANGE",6);
	ZREVRANGE = createStringObject("ZRANGE",9);
	ZCARD = createStringObject("ZCARD",5);
	DUMP = createStringObject("DUMP",4);
	RESTORE =  createStringObject("RESTORE",7);

	
	
	for (j = 0; j < REDIS_SHARED_INTEGERS; j++)
	{
		integers[j] = createObject(REDIS_STRING,(void*)(long)j);
		integers[j]->encoding = REDIS_ENCODING_INT;
	}


	for (j = 0; j < REDIS_SHARED_BULKHDR_LEN; j++)
	{
		mbulkhdr[j] = createObject(REDIS_STRING,sdscatprintf(sdsempty(),"*%d\r\n",j));
		bulkhdr[j] = createObject(REDIS_STRING,sdscatprintf(sdsempty(),"$%d\r\n",j));
	}

	char buf[8];
	int32_t len = ll2string(buf,sizeof(buf),redis->getPort());
	rPort = createStringObject(buf,len);
	rIp = createStringObject((char*)(redis->getIp().c_str()),redis->getIp().length());

#define REGISTER_REDIS_COMMAND(msgId, func) \
    msgId->calHash(); \
	redis->handlerCommands[msgId] = std::bind(&xRedis::func, redis, std::placeholders::_1, std::placeholders::_2);
	REGISTER_REDIS_COMMAND(set,setCommand);
	REGISTER_REDIS_COMMAND(get,getCommand);
	REGISTER_REDIS_COMMAND(hset,hsetCommand);
	REGISTER_REDIS_COMMAND(hget,hgetCommand);
	REGISTER_REDIS_COMMAND(hlen,hlenCommand);
	REGISTER_REDIS_COMMAND(hgetall,hgetallCommand);
	REGISTER_REDIS_COMMAND(lpush,lpushCommand);
	REGISTER_REDIS_COMMAND(rpush,rpushCommand);
	REGISTER_REDIS_COMMAND(lpop,lpopCommand);
	REGISTER_REDIS_COMMAND(rpop,rpopCommand);
	REGISTER_REDIS_COMMAND(lrange,lrangeCommand);
	REGISTER_REDIS_COMMAND(rpop,rpopCommand);
	REGISTER_REDIS_COMMAND(llen,llenCommand);
	REGISTER_REDIS_COMMAND(zadd,zaddCommand);
	REGISTER_REDIS_COMMAND(zrange,zrangeCommand);
	REGISTER_REDIS_COMMAND(zcard,zcardCommand);
	REGISTER_REDIS_COMMAND(zrevrange,zrevrangeCommand);
	REGISTER_REDIS_COMMAND(scard,scardCommand);
	REGISTER_REDIS_COMMAND(sadd,saddCommand);
	REGISTER_REDIS_COMMAND(dump,dumpCommand);
	REGISTER_REDIS_COMMAND(restore,restoreCommand);

	REGISTER_REDIS_COMMAND(flushdb,flushdbCommand);
	REGISTER_REDIS_COMMAND(dbsize,dbsizeCommand);
	REGISTER_REDIS_COMMAND(ping,pingCommand);
	REGISTER_REDIS_COMMAND(save,saveCommand);
	REGISTER_REDIS_COMMAND(slaveof,slaveofCommand);
	REGISTER_REDIS_COMMAND(sync,syncCommand);
	REGISTER_REDIS_COMMAND(command,commandCommand);
	REGISTER_REDIS_COMMAND(config,configCommand);
	REGISTER_REDIS_COMMAND(auth,authCommand);
	REGISTER_REDIS_COMMAND(info,infoCommand);
	REGISTER_REDIS_COMMAND(echo,echoCommand);
	REGISTER_REDIS_COMMAND(client,clientCommand);
	REGISTER_REDIS_COMMAND(del,delCommand);
	REGISTER_REDIS_COMMAND(keys,keysCommand);
	REGISTER_REDIS_COMMAND(bgsave,bgsaveCommand);
	REGISTER_REDIS_COMMAND(memory,memoryCommand);
	REGISTER_REDIS_COMMAND(cluster,clusterCommand);
	REGISTER_REDIS_COMMAND(migrate,migrateCommand);
	REGISTER_REDIS_COMMAND(debug,debugCommand);
	REGISTER_REDIS_COMMAND(ttl,ttlCommand);
	REGISTER_REDIS_COMMAND(SET,setCommand);
	REGISTER_REDIS_COMMAND(GET,getCommand);
	REGISTER_REDIS_COMMAND(HSET,hsetCommand);
	REGISTER_REDIS_COMMAND(HGET,hgetCommand);
	REGISTER_REDIS_COMMAND(HLEN,hlenCommand);
	REGISTER_REDIS_COMMAND(HGETALL,hgetallCommand);
	REGISTER_REDIS_COMMAND(LPUSH,lpushCommand);
	REGISTER_REDIS_COMMAND(RPUSH,rpushCommand);
	REGISTER_REDIS_COMMAND(LPOP,lpopCommand);
	REGISTER_REDIS_COMMAND(RPOP,rpopCommand);
	REGISTER_REDIS_COMMAND(LRANGE,lrangeCommand);
	REGISTER_REDIS_COMMAND(RPOP,rpopCommand);
	REGISTER_REDIS_COMMAND(LLEN,llenCommand);
	REGISTER_REDIS_COMMAND(ZADD,zaddCommand);
	REGISTER_REDIS_COMMAND(ZRANGE,zrangeCommand);
	REGISTER_REDIS_COMMAND(ZCARD,zcardCommand);
	REGISTER_REDIS_COMMAND(ZREVRANGE,zrevrangeCommand);
	REGISTER_REDIS_COMMAND(SCARD,scardCommand);
	REGISTER_REDIS_COMMAND(SADD,saddCommand);
	REGISTER_REDIS_COMMAND(DUMP,dumpCommand);
	REGISTER_REDIS_COMMAND(RESTORE,restoreCommand);
	REGISTER_REDIS_COMMAND(FLUSHDB,flushdbCommand);
	REGISTER_REDIS_COMMAND(DBSIZE,dbsizeCommand);
	REGISTER_REDIS_COMMAND(PING,pingCommand);
	REGISTER_REDIS_COMMAND(SAVE,saveCommand);
	REGISTER_REDIS_COMMAND(SLAVEOF,slaveofCommand);
	REGISTER_REDIS_COMMAND(SYNC,syncCommand);
	REGISTER_REDIS_COMMAND(COMMAND,commandCommand);
	REGISTER_REDIS_COMMAND(CONFIG,configCommand);
	REGISTER_REDIS_COMMAND(AUTH,authCommand);
	REGISTER_REDIS_COMMAND(INFO,infoCommand);
	REGISTER_REDIS_COMMAND(ECHO,echoCommand);
	REGISTER_REDIS_COMMAND(CLIENT,clientCommand);
	REGISTER_REDIS_COMMAND(DEL,delCommand);
	REGISTER_REDIS_COMMAND(KEYS,keysCommand);
	REGISTER_REDIS_COMMAND(BGSAVE,bgsaveCommand);
	REGISTER_REDIS_COMMAND(MEMORY,memoryCommand);
	REGISTER_REDIS_COMMAND(CLUSTER,clusterCommand);
	REGISTER_REDIS_COMMAND(MIGRATE,migrateCommand);
	REGISTER_REDIS_COMMAND(DEBUG,debugCommand);
	REGISTER_REDIS_COMMAND(TTL,ttlCommand);

#define REGISTER_REDIS_REPLY_COMMAND(msgId) \
	msgId->calHash(); \
	redis->replyCommands.insert(msgId);
	REGISTER_REDIS_REPLY_COMMAND(addsync);
	REGISTER_REDIS_REPLY_COMMAND(setslot);
	REGISTER_REDIS_REPLY_COMMAND(node);
	REGISTER_REDIS_REPLY_COMMAND(connect);
	REGISTER_REDIS_REPLY_COMMAND(delsync);
	REGISTER_REDIS_REPLY_COMMAND(cluster);
	REGISTER_REDIS_REPLY_COMMAND(rIp);
	REGISTER_REDIS_REPLY_COMMAND(rPort);

#define REGISTER_REDIS_CHECK_COMMAND(msgId) \
	msgId->calHash(); \
	redis->replyCommands.insert(msgId);
	REGISTER_REDIS_CHECK_COMMAND(set);
	REGISTER_REDIS_CHECK_COMMAND(hset);
	REGISTER_REDIS_CHECK_COMMAND(lpush);
	REGISTER_REDIS_CHECK_COMMAND(rpush);
	REGISTER_REDIS_CHECK_COMMAND(sadd);
	REGISTER_REDIS_CHECK_COMMAND(lpop);
	REGISTER_REDIS_CHECK_COMMAND(rpop);
	REGISTER_REDIS_CHECK_COMMAND(del);
	REGISTER_REDIS_CHECK_COMMAND(flushdb);

#define REGISTER_REDIS_CLUSTER_CHECK_COMMAND(msgId) \
	msgId->calHash(); \
	redis->cluterCommands.insert(msgId);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(cluster);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(migrate);
	REGISTER_REDIS_CLUSTER_CHECK_COMMAND(command);
}

rObj * xObjects::createStringObject(char *ptr, size_t len)
{
	return createEmbeddedStringObject(ptr,len);
}

rObj * xObjects::createRawStringObject(char *ptr, size_t len)
{
	return createObject(REDIS_STRING,sdsnewlen(ptr,len));
}

rObj * xObjects::createEmbeddedStringObject(char *ptr, size_t len)
{
	rObj *o = (rObj*)zmalloc(sizeof(rObj)+sizeof(struct sdshdr)+len+1);
	struct sdshdr *sh = (sdshdr*)(o+1);

	o->type = REDIS_NULL;
	o->encoding = REDIS_ENCODING_EMBSTR;
	o->ptr = (char*)(sh+1);
	o->hash = 0;
	sh->len = len;
	sh->free = 0;
	if (ptr)
	{
	    memcpy(sh->buf,ptr,len);
	    sh->buf[len] = '\0';
	}
	else
	{
	    memset(sh->buf,0,len+1);
	}
   	return o;
}

void xObjects::addReplyBulkLen(xBuffer &buffer,rObj *obj)
{
	size_t len;

	if (sdsEncodedObject(obj))
	{
	    len = sdslen((const sds)obj->ptr);
	}
	else
	{
	    long n = (long)obj->ptr;
	    len = 1;
	    if (n < 0)
	    {
	        len++;
	        n = -n;
	    }
	    while((n = n/10) != 0)
	    {
	        len++;
	    }
	}

	if (len < REDIS_SHARED_BULKHDR_LEN)
	    addReply(buffer,bulkhdr[len]);
	else
	    addReplyLongLongWithPrefix(buffer,len,'$');

}

void xObjects::addReplyBulk(xBuffer &buffer,rObj *obj)
{
	addReplyBulkLen(buffer,obj);
	addReply(buffer,obj);
	addReply(buffer,crlf);
}

void xObjects::addReplyLongLongWithPrefix(xBuffer &buffer,int64_t ll, char prefix)
{
	char buf[128];
	int len;
	if (prefix == '*' && ll < REDIS_SHARED_BULKHDR_LEN) 
	{
	    addReply(buffer,mbulkhdr[ll]);
	    return;
	} 
	else if (prefix == '$' && ll < REDIS_SHARED_BULKHDR_LEN) 
	{
	    addReply(buffer,bulkhdr[ll]);
	    return;
	}

	buf[0] = prefix;
	len = ll2string(buf+1,sizeof(buf)-1,ll);
	buf[len+1] = '\r';
	buf[len+2] = '\n';
	buffer.append(buf,len +3);

}

void xObjects::addReplyLongLong(xBuffer &buffer,size_t len)
{
	if (len == 0)
		addReply(buffer,czero);
	else if (len == 1)
		addReply(buffer,cone);
	else
		addReplyLongLongWithPrefix(buffer,len,':');
}

void xObjects::addReplyStatusLength(xBuffer &buffer, char *s, size_t len)
{
	addReplyString(buffer,"+",1);
	addReplyString(buffer,s,len);
	addReplyString(buffer,"\r\n",2);
}

void xObjects::addReplyStatus(xBuffer &buffer, char *status)
{
    addReplyStatusLength(buffer,status,strlen(status));
}

void xObjects::addReplyError(xBuffer &buffer,const char *str)
{
	addReplyErrorLength(buffer,str,strlen(str));
}

void xObjects::addReply(xBuffer &buffer,rObj *obj)
{
	buffer.append(obj->ptr,sdslen((const sds)obj->ptr));
}

/* Add sds to reply (takes ownership of sds and frees it) */
void xObjects::addReplyBulkSds(xBuffer &buffer, sds s)
{
	addReplySds(buffer,sdscatfmt(sdsempty(),"$%u\r\n",
	    (unsigned long)sdslen(s)));
	addReplySds(buffer,s);
	addReply(buffer,crlf);
}

void xObjects::addReplyMultiBulkLen(xBuffer &buffer,int32_t length)
{
	if (length < REDIS_SHARED_BULKHDR_LEN)
        addReply(buffer,mbulkhdr[length]);
    else
        addReplyLongLongWithPrefix(buffer,length,'*');
}

void xObjects::prePendReplyLongLongWithPrefix(xBuffer &buffer,int32_t length)
{
	char buf[128];
	buf[0] = '*';
	int len = ll2string(buf+1,sizeof(buf)-1,length);
	buf[len+1] = '\r';
	buf[len+2] = '\n';
	if(length == 0)
	{
		buffer.append(buf,len + 3);
	}
	else
	{
		buffer.prepend(buf,len + 3);
	}
}

void xObjects::addReplyBulkCString(xBuffer &buffer, const char *s)
{
	if (s == nullptr)
	{
		addReply(buffer, nullbulk);
	}
	else 
	{
		addReplyBulkCBuffer(buffer, s, strlen(s));
	}
}

void xObjects::addReplyDouble(xBuffer &buffer, double d)
{
	char dbuf[128], sbuf[128];
	int dlen, slen;
	dlen = snprintf(dbuf, sizeof(dbuf), "%.17g", d);
	slen = snprintf(sbuf, sizeof(sbuf), "$%d\r\n%s\r\n", dlen, dbuf);
	addReplyString(buffer, sbuf, slen);
}

void xObjects::addReplyBulkCBuffer(xBuffer &buffer,const char *p, size_t len)
{
	addReplyLongLongWithPrefix(buffer,len,'$');
	addReplyString(buffer,p,len);
	addReply(buffer,crlf);
}

void xObjects::addReplyErrorFormat(xBuffer &buffer,const char *fmt, ...)
{
	size_t l, j;
	va_list ap;
	va_start(ap,fmt);
	sds s = sdscatvprintf(sdsempty(),fmt,ap);
	va_end(ap);
	l = sdslen(s);
	for (j = 0; j < l; j++)
	{
	    if (s[j] == '\r' || s[j] == '\n') s[j] = ' ';
	}
	addReplyErrorLength(buffer,s,sdslen(s));
	sdsfree(s);
}

void xObjects::addReplyString(xBuffer &buffer,const char *s, size_t len)
{
	buffer.append(s,len);
}

void xObjects::addReplySds(xBuffer &buffer, sds s)
{
	buffer.append(s, sdslen(s));
	sdsfree(s);
}


void xObjects::addReplyErrorLength(xBuffer &buffer,const char *s,size_t len)
{
	addReplyString(buffer,"-ERR ",5);
	addReplyString(buffer,s,len);
	addReplyString(buffer,"\r\n",2);
}








