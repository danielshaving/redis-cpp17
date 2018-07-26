#include "object.h"

struct SharedObjectsStruct shared;

void RedisObject::calHash()
{
	hash = dictGenHashFunction(ptr,sdslen(ptr));
}

bool RedisObject::operator <(const RedisObjectPtr &r) const
{
	auto cmp = memcmp(ptr,r->ptr,sdslen(ptr));
	if (cmp < 0)
	{
		return true;
	}
	else if (cmp == 0)
	{
		return memcmp(ptr,r->ptr,sdslen(ptr)) < 0;
	}
	else
	{
		return false;
	}
}

RedisObjectPtr createObject(int32_t type,char *ptr)
{
	RedisObjectPtr o(new RedisObject());
	o->encoding = REDIS_ENCODING_RAW;
	o->type = type;
	o->ptr = ptr;
	o->calHash();
	return o;
}

int32_t getLongLongFromObject(const RedisObjectPtr &o,int64_t *target)
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
			if (string2ll(o->ptr,sdslen(o->ptr),&value) == 0)
			{
				return REDIS_ERR;
			}
		}
		else if (o->encoding == OBJ_ENCODING_INT)
		{
			if (string2ll(o->ptr,sdslen(o->ptr),&value) == 0)
			{
				return REDIS_ERR;
			}
		}
		else
		{
			assert(false);
		}
	}

	if (target)
	{
		*target = value;
	}
	return REDIS_OK;
}

int32_t getLongLongFromObjectOrReply(Buffer &buffer,const RedisObjectPtr &o,int64_t *target,const char *msg)
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

int32_t getLongFromObjectOrReply(Buffer &buffer,const RedisObjectPtr &o,int32_t *target,const char *msg)
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

RedisObjectPtr createStringObjectFromLongLong(int64_t value)
{
	RedisObjectPtr o;
	if (value >= 0 && value < REDIS_SHARED_INTEGERS)
	{
		o = shared.integers[value - 1];
	}
	else
	{
		o = createObject(REDIS_STRING,sdsfromlonglong(value));
	}
	return o;
}

int32_t getDoubleFromObjectOrReply(Buffer &buffer,const RedisObjectPtr &o,double *target,const char *msg)
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

int32_t getDoubleFromObject(const RedisObjectPtr &o,double *target)
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
        	assert(false);
        }
    }

    *target = value;
    return REDIS_OK;
}

void createSharedObjects()
{
	int32_t j;
	shared.crlf = createObject(REDIS_STRING,sdsnew("\r\n"));
	shared.ok = createObject(REDIS_STRING,sdsnew("+OK\r\n"));
	shared.err = createObject(REDIS_STRING,sdsnew("-ERR\r\n"));
	shared.emptybulk = createObject(REDIS_STRING,sdsnew("$0\r\n\r\n"));
	shared.czero = createObject(REDIS_STRING,sdsnew(":0\r\n"));
	shared.cone = createObject(REDIS_STRING,sdsnew(":1\r\n"));
	shared.cnegone = createObject(REDIS_STRING,sdsnew(":-1\r\n"));
	shared.nullbulk = createObject(REDIS_STRING,sdsnew("$-1\r\n"));
	shared.nullmultibulk = createObject(REDIS_STRING,sdsnew("*-1\r\n"));
	shared.emptymultibulk = createObject(REDIS_STRING,sdsnew("*0\r\n"));
	shared.pping = createObject(REDIS_STRING, sdsnew("PPING\r\n"));
	shared.ping = createObject(REDIS_STRING,sdsnew("PING\r\n"));
	shared.pong = createObject(REDIS_STRING, sdsnew("+PONG\r\n"));
	shared.ppong = createObject(REDIS_STRING,sdsnew("PPONG"));
	shared.queued = createObject(REDIS_STRING,sdsnew("+QUEUED\r\n"));
	shared.emptyscan = createObject(REDIS_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));

	shared.wrongtypeerr = createObject(REDIS_STRING,sdsnew(
	    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
	shared.nokeyerr = createObject(REDIS_STRING,sdsnew(
	    "-ERR no such key\r\n"));
	shared.syntaxerr = createObject(REDIS_STRING,sdsnew(
	    "-ERR syntax error\r\n"));
	shared.sameobjecterr = createObject(REDIS_STRING,sdsnew(
	    "-ERR source and destination objects are the same\r\n"));
	shared.outofrangeerr = createObject(REDIS_STRING,sdsnew(
	    "-ERR index out of range\r\n"));
	shared.noscripterr = createObject(REDIS_STRING,sdsnew(
	    "-NOSCRIPT No matching script. Please use EVAL.\r\n"));
	shared.loadingerr = createObject(REDIS_STRING,sdsnew(
	    "-LOADING Redis is loading the dataset in memory\r\n"));
	shared.slowscripterr = createObject(REDIS_STRING,sdsnew(
	    "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
	shared.masterdownerr = createObject(REDIS_STRING,sdsnew(
	    "-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n"));
	shared.bgsaveerr = createObject(REDIS_STRING,sdsnew(
	    "-MISCONF Redis is configured to save RDB snapshots, but is currently no able to persist on disk. Commands that may modify the data set are disabled. Please check Redis logs for details about the error.\r\n"));
	shared.roslaveerr = createObject(REDIS_STRING,sdsnew(
	    "-READONLY You can't write against a read only slave.\r\n"));
	shared.noautherr = createObject(REDIS_STRING,sdsnew(
	    "-NOAUTH Authentication required.\r\n"));
	shared.oomerr = createObject(REDIS_STRING,sdsnew(
	    "-OOM command no allowed when used memory > 'maxmemory'.\r\n"));
	shared.execaborterr = createObject(REDIS_STRING,sdsnew(
	    "-EXECABORT Transaction discarded because of previous errors.\r\n"));
	shared.noreplicaserr = createObject(REDIS_STRING,sdsnew(
	    "-NOREPLICAS Not enough good slaves to write.\r\n"));
	shared.busykeyerr = createObject(REDIS_STRING,sdsnew(
	    "-BUSYKEY Target key name already exists.\r\n"));

	shared.space = createObject(REDIS_STRING,sdsnew(" "));
	shared.colon = createObject(REDIS_STRING,sdsnew(":"));
	shared.plus = createObject(REDIS_STRING,sdsnew("+"));
	shared.asking = createObject(REDIS_STRING,sdsnew("asking"));

	shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
	shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
	shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
	shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
	shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
	shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);

	shared.del = createStringObject("del",3);
	shared.rpop = createStringObject("rpop",4);
	shared.lpop = createStringObject("lpop",4);
	shared.lpush = createStringObject("lpush",5);
	shared.rpush = createStringObject("rpush",5);
	shared.set = createStringObject("set",3);
	shared.get = createStringObject("get",3);
	shared.flushdb = createStringObject("flushdb",7);
	shared.dbsize = createStringObject("dbsize",6);
	shared.hset = createStringObject("hset",4);
	shared.hget = createStringObject("hget",4);
	shared.hgetall = createStringObject("hgetall",7);
	shared.save = createStringObject("save",4);
	shared.slaveof = createStringObject("slaveof",7);
	shared.command = createStringObject("command",7);
	shared.config = createStringObject("config",6);
	shared.auth = createStringObject("rpush",5);
	shared.info = createStringObject("info",4);
	shared.echo = createStringObject("echo",4);
	shared.client = createStringObject("client",6);
	shared.hkeys = createStringObject("hkeys",5);
	shared.hlen = createStringObject("hlen",4);
	shared.keys = createStringObject("keys",4);
	shared.bgsave = createStringObject("bgsave",6);
	shared.memory = createStringObject("memory",6);
	shared.cluster = createStringObject("cluster",7);
	shared.migrate = createStringObject("migrate",7);
	shared.debug = createStringObject("debug",5);
	shared.ttl = createStringObject("ttl",3);
	shared.lrange = createStringObject("lrange",6);
	shared.llen = createStringObject("llen",4);
	shared.sadd = createStringObject("sadd",4);
	shared.scard = createStringObject("scard",5);
	shared.addsync = createStringObject("addsync",7);
	shared.setslot = createStringObject("setslot",7);
	shared.node = createStringObject("node",4);
	shared.clusterconnect = createStringObject("clusterconnect",13);
	shared.sync = createStringObject("sync",4);
	shared.psync = createStringObject("psync",5);
	shared.delsync = createStringObject("delsync",7);
	shared.zadd = createStringObject("zadd",4);
	shared.zrange = createStringObject("zrange",6);
	shared.zrevrange = createStringObject("zrevrange",9);
	shared.zcard = createStringObject("zcard",5);
	shared.dump = createStringObject("dump",4);
	shared.restore = createStringObject("restore",7);
	shared.incr = createStringObject("incr",4);
	shared.decr = createStringObject("decr",4);
	
	shared.PING = createStringObject("PING",4);
	shared.DEL = createStringObject("DEL",3);
	shared.RPOP = createStringObject("RPOP",4);
	shared.LPOP = createStringObject("LPOP",4);
	shared.LPUSH = createStringObject("LPUSH",5);
	shared.RPUSH = createStringObject("RPUSH",5);
	shared.SET = createStringObject("SET",3);
	shared.GET = createStringObject("GET",3);
	shared.FLUSHDB = createStringObject("FLUSHDB",7);
	shared.DBSIZE = createStringObject("DBSIZE",6);
	shared.HSET = createStringObject("HSET",4);
	shared.HGET = createStringObject("HGET",4);
	shared.HGETALL = createStringObject("HGETALL",7);
	shared.SAVE = createStringObject("SAVE",4);
	shared.SLAVEOF = createStringObject("SLAVEOF",7);
	shared.COMMAND = createStringObject("COMMAND",7);
	shared.CONFIG = createStringObject("CONFIG",6);
	shared.AUTH = createStringObject("RPUSH",5);
	shared.INFO = createStringObject("INFO",4);
	shared.ECHO = createStringObject("ECHO",4);
	shared.CLIENT = createStringObject("CLIENT",6);
	shared.HKEYS = createStringObject("HKEYS",5);
	shared.HLEN = createStringObject("HLEN",4);
	shared.KEYS = createStringObject("KEYS",4);
	shared.BGSAVE = createStringObject("BGSAVE",6);
	shared.MEMORY = createStringObject("MEMORY",6);
	shared.CLUSTER = createStringObject("CLUSTER",7);
	shared.MIGRATE = createStringObject("MIGRATE",7);
	shared.DEBUG = createStringObject("DEBUG",5);
	shared.TTL = createStringObject("TTL",3);
	shared.LRANGE = createStringObject("LRANGE",6);
	shared.LLEN = createStringObject("LLEN",4);
	shared.SADD = createStringObject("SADD",4);
	shared.SCARD = createStringObject("SCARD",5);
	shared.ADDSYNC = createStringObject("ADDSYNC",7);
	shared.SETSLOT = createStringObject("SETSLOT",7);
	shared.NODE = createStringObject("NODE",4);
	shared.CONNECT = createStringObject("CONNECT",7);
	shared.PSYNC = createStringObject("PSYNC",5);
	shared.SYNC = createStringObject("SYNC",4);
	shared.DELSYNC = createStringObject("DELSYNC",7);
	shared.ZADD = createStringObject("ZADD",4);
	shared.ZRANGE = createStringObject("ZRANGE",6);
	shared.ZREVRANGE = createStringObject("ZRANGE",9);
	shared.ZCARD = createStringObject("ZCARD",5);
	shared.DUMP = createStringObject("DUMP",4);
	shared.RESTORE = createStringObject("RESTORE",7);
	shared.INCR = createStringObject("INCR",4);
	shared.DECR = createStringObject("DECR",4);
	
	for (j = 0; j < REDIS_SHARED_INTEGERS; j++)
	{
		shared.integers[j] = createObject(REDIS_STRING,sdsfromlonglong(j));
		shared.integers[j]->encoding = REDIS_ENCODING_INT;
	}

	for (j = 0; j < REDIS_SHARED_BULKHDR_LEN; j++)
	{
		shared.mbulkhdr[j] = createObject(REDIS_STRING,sdscatprintf(sdsempty(),"*%d\r\n",j));
		shared.bulkhdr[j] = createObject(REDIS_STRING,sdscatprintf(sdsempty(),"$%d\r\n",j));
	}
}

RedisObjectPtr createStringObject(char *ptr,size_t len)
{
	return createRawStringObject(ptr,len);
}

RedisObjectPtr createRawStringObject(int32_t type,char *ptr,size_t len)
{
	return createObject(type,sdsnewlen(ptr,len));
}

RedisObjectPtr createRawStringObject(char *ptr,size_t len)
{
	return createObject(REDIS_STRING,sdsnewlen(ptr,len));
}

void addReplyBulkLen(Buffer &buffer,const RedisObjectPtr &obj)
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
	{
		addReply(buffer,shared.bulkhdr[len]);
	}
	else
	{
		addReplyLongLongWithPrefix(buffer,len,'$');
	}
}

void addReplyBulk(Buffer &buffer,const RedisObjectPtr &obj)
{
	addReplyBulkLen(buffer,obj);
	addReply(buffer,obj);
	addReply(buffer,shared.crlf);
}

void addReplyLongLongWithPrefix(Buffer &buffer,int64_t ll,char prefix)
{
	char buf[128];
	int32_t len;
	if (prefix == '*' && ll < REDIS_SHARED_BULKHDR_LEN)
	{
		addReply(buffer,shared.mbulkhdr[ll]);
		return;
	}
	else if (prefix == '$' && ll < REDIS_SHARED_BULKHDR_LEN)
	{
		addReply(buffer,shared.bulkhdr[ll]);
		return;
	}

	buf[0] = prefix;
	len = ll2string(buf+1,sizeof(buf)-1,ll);
	buf[len+1] = '\r';
	buf[len+2] = '\n';
	buffer.append(buf,len +3);
}

void addReplyLongLong(Buffer &buffer,size_t len)
{
	if (len == 0)
	{
		addReply(buffer,shared.czero);
	}
	else if (len == 1)
	{
		addReply(buffer,shared.cone);
	}
	else
	{
		addReplyLongLongWithPrefix(buffer,len,':');
	}
}

void addReplyStatusLength(Buffer &buffer,char *s,size_t len)
{
	addReplyString(buffer,"+",1);
	addReplyString(buffer,s,len);
	addReplyString(buffer,"\r\n",2);
}

void addReplyStatus(Buffer &buffer,char *status)
{
    addReplyStatusLength(buffer,status,strlen(status));
}

void addReplyError(Buffer &buffer,const char *str)
{
	addReplyErrorLength(buffer,str,strlen(str));
}

void addReply(Buffer &buffer,const RedisObjectPtr &obj)
{
	buffer.append(obj->ptr,sdslen((const sds)obj->ptr));
}

/* Add sds to reply (takes ownership of sds and frees it) */
void addReplyBulkSds(Buffer &buffer,sds s)
{
	addReplySds(buffer,sdscatfmt(sdsempty(),"$%u\r\n",(unsigned long)sdslen(s)));
	addReplySds(buffer,s);
	addReply(buffer,shared.crlf);
}

void addReplyMultiBulkLen(Buffer &buffer,int32_t length)
{
	if (length < REDIS_SHARED_BULKHDR_LEN)
	{
		addReply(buffer,shared.mbulkhdr[length]);
	}
    else
    {
    	addReplyLongLongWithPrefix(buffer,length,'*');
    }
}

void prePendReplyLongLongWithPrefix(Buffer &buffer,int32_t length)
{
	char buf[128];
	buf[0] = '*';
	int32_t len = ll2string(buf+1,sizeof(buf)-1,length);
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

void addReplyBulkCString(Buffer &buffer,const char *s)
{
	if (s == nullptr)
	{
		addReply(buffer,shared.nullbulk);
	}
	else
	{
		addReplyBulkCBuffer(buffer,s,strlen(s));
	}
}

void addReplyDouble(Buffer &buffer,double d)
{
	char dbuf[128],sbuf[128];
	int32_t dlen,slen;
	dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
	slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
	addReplyString(buffer,sbuf,slen);
}

void addReplyBulkCBuffer(Buffer &buffer,const char *p,size_t len)
{
	addReplyLongLongWithPrefix(buffer,len,'$');
	addReplyString(buffer,p,len);
	addReply(buffer,shared.crlf);
}

void addReplyErrorFormat(Buffer &buffer,const char *fmt, ...)
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

void addReplyString(Buffer &buffer,const char *s,size_t len)
{
	buffer.append(s,len);
}

void addReplySds(Buffer &buffer,sds s)
{
	buffer.append(s,sdslen(s));
	sdsfree(s);
}

void addReplyErrorLength(Buffer &buffer,const char *s,size_t len)
{
	addReplyString(buffer,"-ERR ",5);
	addReplyString(buffer,s,len);
	addReplyString(buffer,"\r\n",2);
}








