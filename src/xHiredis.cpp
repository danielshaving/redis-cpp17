#include "xHiredis.h"

xRedisReader::xRedisReader(xBuffer *buffer)
:buffer(buffer)
{
	pos = 0;
	err = 0;
	errstr[0] = '\0';
	ridx = -1;
}

xRedisReader::xRedisReader()
{
	xBuffer buf;
	buffer = &buf;
	pos = 0;
	err = 0;
	errstr[0] = '\0';
	ridx = -1;
}

xRedisReader::~xRedisReader()
{

}


xRedisContext::xRedisContext()
:reader(new xRedisReader())
{
	flags &= ~REDIS_BLOCK;
	err = 0;
	errstr[0] = '\0';
	fd = 0;
}

xRedisContext::xRedisContext(xBuffer *buffer,int32_t sockfd)
:reader(new xRedisReader(buffer)),
 fd(sockfd)
{
	flags &= ~REDIS_BLOCK;
	err = 0;
	errstr[0] = '\0';
	fd = 0;
}

xRedisContext::~xRedisContext()
{

}

void xRedisContext::setBlock()
{
	flags |= REDIS_BLOCK;
}

void xRedisContext::setDisConnected()
{
	flags &= REDIS_CONNECTED;
}

void xRedisContext::setConnected()
{
	flags != REDIS_CONNECTED;
}

xRedisAsyncContext::xRedisAsyncContext(xBuffer *buffer,const TcpConnectionPtr &conn)
:c(new xRedisContext(buffer,conn->getSockfd())),
 serverConn(conn),
 err(0),
 errstr(nullptr),
 data(nullptr)
{
	c->setConnected();
}

xRedisAsyncContext::~xRedisAsyncContext()
{
	for(auto &it : asyncCb)
	{
		if(it.data)
		{
			zfree(it.data);
		}
	}
}

 void  xRedisReader::redisReaderSetError(int32_t type, const char *str)
{
	size_t len;
	if (reply != nullptr && fn.freeObjectFuc)
	{
		fn.freeObjectFuc(reply);
		reply = nullptr;
	}

	/* Clear input buffer on errors. */
	if (buffer != nullptr)
	{
		buffer->retrieveAll();
		pos = 0;
	}

	/* Reset task stack. */
	ridx = -1;

	/* Set error. */
	err = type;
	len = strlen(str);
	len = len < (sizeof(errstr)-1) ? len : (sizeof(errstr)-1);
	memcpy(errstr,str,len);
	errstr[len] = '\0';
}


static size_t chrtos(char *buffer, size_t size, char byte)
{
    size_t len = 0;

    switch(byte)
    {
		case '\\':
		case '"':
			len = snprintf(buffer,size,"\"\\%c\"",byte);
			break;
		case '\n': len = snprintf(buffer,size,"\"\\n\""); break;
		case '\r': len = snprintf(buffer,size,"\"\\r\""); break;
		case '\t': len = snprintf(buffer,size,"\"\\t\""); break;
		case '\a': len = snprintf(buffer,size,"\"\\a\""); break;
		case '\b': len = snprintf(buffer,size,"\"\\b\""); break;
		default:
			if (isprint(byte))
				len = snprintf(buffer,size,"\"%c\"",byte);
			else
				len = snprintf(buffer,size,"\"\\x%02x\"",(unsigned char)byte);
			break;
    }

    return len;
}


void xRedisReader::redisReaderSetErrorProtocolByte(char byte)
{
    char cbuf[8], sbuf[128];
    chrtos(cbuf,sizeof(cbuf),byte);
    snprintf(sbuf,sizeof(sbuf),"Protocol error, got %s as reply type byte", cbuf);
    redisReaderSetError(REDIS_ERR_PROTOCOL,sbuf);
}

void xRedisReader::redisReaderSetErrorOOM()
{
    redisReaderSetError(REDIS_ERR_OOM,"Out of memory");
}

static int32_t intlen(int32_t i)
{
	int32_t len = 0;
	if (i < 0)
	{
		len++;
		i = -i;
	}

	do
	{
		len++;
		i /= 10;
	}while(i);

	return len;
}


/* Create a reply object */
redisReply * createReplyObject(int32_t type)
{
	redisReply *r = (redisReply*)zcalloc( sizeof(*r));

	if (r == nullptr)
		return nullptr;

	r->type = type;
	return r;
}

int64_t xRedisReader::readLongLong(const char *s)
{
	int64_t v = 0;
	int32_t  dec, mult = 1;
	char	c;

	if (*s == '-')
	{
		mult	= -1;
		s++;
	}
	else if (*s == '+')
	{
		mult	= 1;
		s++;
	}

	while ((c = * (s++)) != '\r')
	{
		dec = c - '0';

		if (dec >= 0 && dec < 10)
		{
			v *= 10;
			v += dec;
		}
		else
		{
			/* Should not happen... */
			return - 1;
		}
	}

	return mult * v;
}


/* Find pointer to \r\n. */
static const char * seekNewline(const char *s, size_t len)
{
	int32_t 	pos = 0;
	int32_t 	_len = len - 1;

	/* Position should be < len-1 because the character at "pos" should be
	 * followed by a \n. Note that strchr cannot be used because it doesn't
	 * allow to search a limited length and the buffer that is being searched
	 * might not have a trailing nullptr character. */
	while (pos < _len)
	{
		while (pos < _len && s[pos] != '\r')
			pos++;

		if (s[pos] != '\r')
		{
			/* Not found. */
			return nullptr;
		}
		else
		{
			if (s[pos + 1] == '\n')
			{
				/* Found. */
				return s + pos;
			}
			else
			{
				/* Continue searching. */
				pos++;
			}
		}
	}

	return nullptr;
}


const char * xRedisReader::readLine(int32_t * _len)
{
	const char * p;
	const char *s;
	int32_t len;

	p = buffer->peek() + pos;
	s = seekNewline(p, (buffer->readableBytes() - pos));

	if (s != nullptr)
	{
		len = s - (buffer->peek() + pos);
		pos += len + 2; 			/* skip \r\n */

		if (_len)
			*_len = len;

		return p;
	}

	return nullptr;
}

const char * xRedisReader::readBytes(uint32_t bytes)
{
	const char * p;
	if (buffer->readableBytes() - pos >= bytes)
	{
		p = buffer->peek() + pos;
		pos += bytes;
		return p;
	}

	return nullptr;
}


/* Free a reply object */
void freeReply(redisReply  *reply)
{
	redisReply *r = reply;
	size_t	 j;

	if(r == nullptr)
	{
		return ;
	}

	switch (r->type)
	{
		case REDIS_REPLY_INTEGER:
			break; /* Nothing to free */

		case REDIS_REPLY_ARRAY:
			if (r->element != nullptr)
			{
				for (j = 0; j < r->elements; j++)
					if (r->element[j] != nullptr)
						freeReply(r->element[j]);

				zfree(r->element);
			}

			break;

		case REDIS_REPLY_ERROR:
		case REDIS_REPLY_STATUS:
		case REDIS_REPLY_STRING:
			if (r->str != nullptr)
				zfree(r->str);

			break;
	}
	zfree(r);
}

redisReply *createString(const redisReadTask *task, const char *str, size_t len)
{
	redisReply *r, *parent;
	char * buffer;

	r = createReplyObject(task->type);

	if (r == nullptr)
		return nullptr;

	buffer  = (char *)zmalloc(len + 1);

	if (buffer == nullptr)
	{
		freeReply(r);
		return nullptr;
	}

	assert(task->type == REDIS_REPLY_ERROR || task->type == REDIS_REPLY_STATUS || task->type == REDIS_REPLY_STRING);

	/* Copy string value */
	memcpy(buffer, str, len);
	buffer[len] = '\0';
	r->str = buffer;
	r->len = len;

	if (task->parent)
	{
		parent =(redisReply*) task->parent->obj;
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}

	return r;
}


redisReply *createArray(const redisReadTask *task, int32_t elements)
{
	redisReply * r, *parent;

	r = createReplyObject(REDIS_REPLY_ARRAY);

	if (r == nullptr)
		return nullptr;

	if (elements > 0)
	{
		r->element	 = (redisReply**)zmalloc(elements * sizeof(redisReply *));

		if (r->element == nullptr)
		{
			freeReply(r);
			return nullptr;
		}
	}

	r->elements = elements;

	if (task->parent)
	{
		parent = (redisReply*)task->parent->obj;
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}

	return r;
}


redisReply *createInteger(const redisReadTask *task, int64_t value)
{
	redisReply *r, *parent;

	r = createReplyObject(REDIS_REPLY_INTEGER);

	if (r == nullptr)
		return nullptr;

	r->integer	= value;

	if (task->parent)
	{
		parent = (redisReply*)task->parent->obj;
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}

	return r;
}


redisReply *createNil(const redisReadTask *task)
{
	redisReply * r, *parent;

	r = createReplyObject(REDIS_REPLY_NIL);

	if (r == nullptr)
		return nullptr;

	if (task->parent)
	{
		parent = (redisReply*)task->parent->obj;
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element[task->idx] = r;
	}

	return r;
}


static size_t bulklen(size_t len)
{
	return 1 + intlen(len) + 2 + len + 2;
}


static char *nextArgument(char *start, char **str, size_t *len)
{
    char *p = start;
    if (p[0] != '$')
    {
        p = strchr(p,'$');
        if (p == nullptr) return nullptr;
    }

    *len = (int32_t)strtol(p+1,nullptr,10);
    p = strchr(p,'\r');
    assert(p);
    *str = p+2;
    return p+2+(*len)+2;
}



void xRedisContext::redisSetError(int32_t type, const char * str)
{
	size_t	len;
	err = type;
	if (str != nullptr)
	{
		len  = strlen(str);
		len  = len < (sizeof(errstr) - 1) ? len: (sizeof(errstr) - 1);
		memcpy(errstr, str, len);
		errstr[len] = '\0';
	}
	else
	{
		assert(type == REDIS_ERR_IO);
		memcpy(errstr, strerror(errno),sizeof(errstr));
	}
}

void xRedisReader::moveToNextTask()
{
	redisReadTask * cur, *prv;
	while (ridx >= 0)
	{
		/* Return a.s.a.p. when the stack is now empty. */
		if (ridx == 0)
		{
			ridx--;
			return;
		}

		cur = & (rstack[ridx]);
		prv = & (rstack[ridx - 1]);
		assert(prv->type == REDIS_REPLY_ARRAY);

		if (cur->idx == prv->elements - 1)
		{
			ridx--;
		}
		else
		{
			/* Reset the type because the next item can be anything */
			assert(cur->idx < prv->elements);
			cur->type = -1;
			cur->elements = -1;
			cur->idx++;
			return;
		}
	}
}

int32_t xRedisReader::processLineItem()
{
	redisReadTask *cur = &(rstack[ridx]);
	redisReply  *obj;
	const char * p;
	int32_t len;

	if ((p = readLine(&len)) != nullptr)
	{
		if (cur->type == REDIS_REPLY_INTEGER)
		{
			if (fn.createIntegerFuc)
				obj = fn.createIntegerFuc(cur, readLongLong(p));
			else
				assert(false);
		}
		else
		{
			/* Type will be error or status. */
			if (fn.createStringFuc)
				obj = fn.createStringFuc(cur, p, len);
			else
				assert(false);
		}

		if (obj == nullptr)
		{
			redisReaderSetErrorOOM();
			return REDIS_ERR;
		}

		/* Set reply if this is the root object. */
		if (ridx == 0)
			reply = obj;

		moveToNextTask();
		return REDIS_OK;
	}

	return REDIS_ERR;
}


int32_t xRedisReader::processBulkItem()
{
	redisReadTask *cur = &(rstack[ridx]);
	redisReply *obj = nullptr;
	const char *p;
	const char *s;
	int32_t	len;
	unsigned long bytelen;
	int32_t success = 0;

	p = buffer->peek() + pos;
	s = seekNewline(p, buffer->readableBytes() - pos);

	if (s != nullptr)
	{
		p = buffer->peek() + pos;
		bytelen = s - (buffer->peek() + pos) + 2; /* include \r\n */
		len = readLongLong(p);

		if (len < 0)
		{
			/* The nil object can always be created. */
			if (fn.createNilFuc)
				obj = fn.createNilFuc(cur);
			else
				assert(false);

				success = 1;
		}
		else
		{
			/* Only continue when the buffer contains the entire bulk item. */
			bytelen += len + 2; 		/* include \r\n */

			if (pos + bytelen <= buffer->readableBytes())
			{
				if (fn.createStringFuc)
					obj = fn.createStringFuc(cur, s + 2, len);
				else
					assert(false);

				success  = 1;
			}
		}

		/* Proceed when obj was created. */
		if (success)
		{
			if (obj == nullptr)
			{
				redisReaderSetErrorOOM();
				return REDIS_ERR;
			}

			pos += bytelen;

			/* Set reply if this is the root object. */
			if (ridx == 0)
				reply = obj;

			moveToNextTask();
			return REDIS_OK;
		}
	}

	return REDIS_ERR;
}


int32_t xRedisReader::processMultiBulkItem()
{
	redisReadTask *cur = &(rstack[ridx]);
	redisReply *obj;
	const char * p;
	int32_t elements;
	int32_t  root = 0;

	/* Set error for nested multi bulks with depth > 7 */
	if (ridx == 8)
	{
		redisReaderSetError(REDIS_ERR_PROTOCOL,"No support for nested multi bulk replies with depth > 7");
		return REDIS_ERR;
	}

	if ((p = readLine(nullptr)) != nullptr)
	{
		elements = readLongLong(p);
		root = (ridx == 0);

		if (elements == -1)
		{
			if (fn.createNilFuc)
				obj = fn.createNilFuc(cur);
			else
				assert(false);

			if (obj == nullptr)
			{
				redisReaderSetErrorOOM();
				return REDIS_ERR;
			}

			moveToNextTask();
		}
		else
		{
			if (fn.createArrayFuc)
				obj = fn.createArrayFuc(cur, elements);
			else
				assert(false);

			if (obj == nullptr)
			{
				redisReaderSetErrorOOM();
				return REDIS_ERR;
			}

			/* Modify task stack when there are more than 0 elements. */
			if (elements > 0)
			{
				cur->elements = elements;
				cur->obj = obj;
				ridx++;
				rstack[ridx].type = -1;
				rstack[ridx].elements = -1;
				rstack[ridx].idx = 0;
				rstack[ridx].obj = nullptr;
				rstack[ridx].parent = cur;
				rstack[ridx].privdata = privdata;
			}
			else
			{
				moveToNextTask();
			}
		}

		/* Set reply if this is the root object. */
		if (root)
			reply = obj;

		return REDIS_OK;
	}

	return REDIS_ERR;
}

int32_t xRedisReader::processItem()
{
	redisReadTask *cur = &(rstack[ridx]);
	const char *p;

/* check if we need to read type */
	if (cur->type < 0)
	{
		if ((p = readBytes(1)) != nullptr)
		{
			switch (p[0])
			{
				case '-':
					cur->type = REDIS_REPLY_ERROR;
					break;

				case '+':
					cur->type = REDIS_REPLY_STATUS;
					break;

				case ':':
					cur->type = REDIS_REPLY_INTEGER;
					break;

				case '$':
					cur->type = REDIS_REPLY_STRING;
					break;

				case '*':
					cur->type = REDIS_REPLY_ARRAY;
					break;

				default:
					/* could not consume 1 byte */
					 redisReaderSetErrorProtocolByte(*((char *)p));
					return REDIS_ERR;
			}
		}
		else
		{
			return REDIS_ERR;
		}
	}

/* process typed item */
	switch (cur->type)
	{
		case REDIS_REPLY_ERROR:
		case REDIS_REPLY_STATUS:
		case REDIS_REPLY_INTEGER:
			return processLineItem();

		case REDIS_REPLY_STRING:
			return processBulkItem();

		case REDIS_REPLY_ARRAY:
			return processMultiBulkItem();

		default:
			return REDIS_ERR; /* Avoid warning. */
	}
}

int32_t xRedisReader::redisReaderGetReply(redisReply **reply)
{
	if (reply != nullptr)
	{
		*reply = nullptr;
	}

	if (err)
	{
		return REDIS_ERR;
	}

	if (buffer->readableBytes() == 0)
	{
		return REDIS_OK;
	}

	if (ridx == -1)
	{
		rstack[0].type = -1;
		rstack[0].elements = -1;
		rstack[0].idx = -1;
		rstack[0].obj = nullptr;
		rstack[0].parent = nullptr;
		rstack[0].privdata = privdata;
		ridx = 0;
	}

	while (ridx >= 0)
	{
		if (processItem() != REDIS_OK)
		{
			break;
		}
	}

	if (err)
	{
		return REDIS_ERR;
	}

	if (pos >= 1024)
	{
		buffer->retrieve(pos);
		pos = 0;
	}

	/* Emit a reply when there is one. */
	if (ridx == -1)
	{
		if (reply != nullptr)
		{
			*reply = this->reply;
		}

		this->reply = nullptr;
	}

	return REDIS_OK;
}

int32_t xRedisContext::redisGetReplyFromReader(redisReply **reply)
{
	if (reader->redisReaderGetReply(reply) == REDIS_ERR)
	{
		redisSetError(reader->err,reader->errstr);
		return REDIS_ERR;
	}

	return REDIS_OK;
}

int32_t xRedisContext::redisBufferWrite( int32_t *done)
{
	int32_t nwritten;
	if (err)
	{
		return REDIS_ERR;
	}

	if (sender.readableBytes() > 0)
	{
		nwritten =  ::write(fd, sender.peek(), sender.readableBytes());
		if (nwritten  < 0 )
		{
			if ((errno == EAGAIN && ! (flags & REDIS_BLOCK)) || (errno == EINTR))
			{
				/* Try again later */
			}
			else
			{
				redisSetError(REDIS_ERR_IO,nullptr);
				return REDIS_ERR;
			}
		}
		else if (nwritten > 0)
		{
			if (nwritten == (signed) sender.readableBytes())
			{
				sender.retrieveAll();
			}
			else
			{
				sender.retrieve(nwritten);
			}
		}
	}

	if (done != nullptr)
	{
		*done =(sender.readableBytes() == 0);
	}

	return REDIS_OK;
}

int32_t xRedisContext::redisBufferRead()
{
	int32_t savedErrno = 0;
	ssize_t n = reader->buffer->readFd(fd,&savedErrno);
	if (n > 0)
	{
		
	}
	else if (n == 0)
	{
		redisSetError(REDIS_ERR_EOF,"Server closed the connection");
		return REDIS_ERR;
	}
	else
	{
		if ((errno == EAGAIN && ! (flags & REDIS_BLOCK)) || (errno == EINTR))
		{

		}
		else
		{
			redisSetError(REDIS_ERR_IO,nullptr);
			return REDIS_ERR;
		}
	}

	return REDIS_OK;
}

int32_t xRedisContext::redisGetReply(redisReply **reply)
{
	int32_t wdone	= 0;
	redisReply *aux = nullptr;

	if(redisGetReplyFromReader(&aux) == REDIS_ERR)
	{
		redisSetError(reader->err,reader->errstr);
		return REDIS_ERR;
	}

	if (aux == nullptr && flags & REDIS_BLOCK)
	{
		do
		{
			if (redisBufferWrite(&wdone) == REDIS_ERR)
			return REDIS_ERR;

		}while(!wdone);

		do
		{
			if (redisBufferRead() == REDIS_ERR)
				return REDIS_ERR;

			if (redisGetReplyFromReader(&aux) == REDIS_ERR)
				return REDIS_ERR;

		}while(aux == nullptr);
	}

	if (reply != nullptr)
		*reply = aux;

	return REDIS_OK;
}


int32_t xRedisContext::redisAppendCommand(const char *format, ...)
{
    va_list ap;
    int32_t ret;

    va_start(ap,format);
    ret = redisvAppendCommand(format,ap);
    va_end(ap);
    return ret;
}

void xRedisAsyncContext::__redisAsyncCommand(const RedisCallbackFn &fn, const std::any &privdata, char *cmd, size_t len)
{
	RedisCallback cb;
	cb.fn = std::move(fn);
	cb.privdata = privdata;

	RedisAsyncCallback call;
	call.data = cmd;
	call.len = len;
	call.cb = std::move(cb);

	{
		std::unique_lock<std::mutex> lk(mtx);
		asyncCb.push_back(std::move(call));
	}

	serverConn->sendPipe(cmd,len);

}


int32_t redisvFormatCommand(char **target, const char * format, va_list ap)
{
	const char *c = format;
	char *cmd = nullptr; 					/* final command */
	int32_t pos;							/* position in final command */
	sds curarg, newarg; 				/* current argument */
	int32_t touched = 0;					/* was the current argument touched? */
	char ** curargv = nullptr, **newargv = nullptr;
	int32_t argc = 0;
	int32_t totlen = 0;
	int32_t j;

	/* Abort if there is not target to set */
	if (target == nullptr)
		return - 1;
	/* Build the command string accordingly to protocol */
	curarg = sdsempty();
	if (curarg == nullptr)
		return - 1;

	while (*c != '\0')
	{
		if (*c != '%' || c[1] == '\0')
		{
			if (*c == ' ')
			{
				if (touched)
				{
					newargv = (char**) zrealloc(curargv, sizeof(char *) * (argc + 1));
					if (newargv == nullptr)
						goto err;
					curargv  = newargv;
					curargv[argc++] = curarg;
					totlen += bulklen(sdslen(curarg));
					/* curarg is put in argv so it can be overwritten. */
					curarg = sdsempty();
					if (curarg == nullptr)
						goto err;
					touched  = 0;
				}
			}
			else
			{
				newarg = sdscatlen(curarg, c, 1);
				if (newarg == nullptr)
					goto err;
				curarg = newarg;
				touched = 1;
			}
		}
		else
		{
			char * arg;
			size_t	 size;
			/* Set newarg so it can be checked even if it is not touched. */
			newarg = curarg;
			switch (c[1])
			{
				case 's':
					arg = va_arg(ap, char *);
					size = strlen(arg);

					if (size > 0)
						newarg = sdscatlen(curarg, arg, size);
					break;
				case 'b':
					arg = va_arg(ap, char *);
					size = va_arg(ap, size_t);
					if (size > 0)
						newarg = sdscatlen(curarg, arg, size);
					break;
				case '%':
					newarg = sdscat(curarg, "%");
					break;
				default:
				/* Try to detect printf format */
				{
					static const char intfmts[] = "diouxX";
					char	 _format[16];
					const char * _p = c + 1;
					size_t	 _l	= 0;
					va_list  _cpy;

					/* Flags */
					if (*_p != '\0' && *_p == '#')
						_p ++;

					if (*_p != '\0' && *_p == '0')
						_p ++;

					if (*_p != '\0' && *_p == '-')
						_p ++;

					if (*_p != '\0' && *_p == ' ')
						_p ++;

					if (*_p != '\0' && *_p == '+')
						_p ++;

					/* Field width */
					while (*_p != '\0' && isdigit(*_p))
					_p ++;

					/* Precision */
					if (*_p == '.')
					{
						_p ++;
						while (*_p != '\0' && isdigit(*_p))
							_p ++;
					}
					/* Copy va_list before consuming with va_arg */
					va_copy(_cpy, ap);
					/* Integer conversion (without modifiers) */
					if (strchr(intfmts, *_p) != nullptr)
					{
						va_arg(ap, int32_t);
						goto fmt_valid;
					}
					/* Double conversion (without modifiers) */
					if (strchr("eEfFgGaA", *_p) != nullptr)
					{
						va_arg(ap, double);
						goto fmt_valid;
					}
					/* Size: char */
					if (_p[0] == 'h' && _p[1] == 'h')
					{
						_p += 2;
						if (*_p != '\0' && strchr(intfmts, *_p) != nullptr)
						{
							va_arg(ap, int32_t);	/* char gets promoted to int32_t */
							goto fmt_valid;
						}
						goto fmt_invalid;
					}
					/* Size: short */
					if (_p[0] == 'h')
					{
						_p	+= 1;
						if (*_p != '\0' && strchr(intfmts, *_p) != nullptr)
						{
							va_arg(ap, int32_t);	/* short gets promoted to int32_t */
							goto fmt_valid;
						}
						goto fmt_invalid;
					}
					/* Size: long long */
					if (_p[0] == 'l' && _p[1] == 'l')
					{
						_p	+= 2;

						if (*_p != '\0' && strchr(intfmts, *_p) != nullptr)
						{
							va_arg(ap, long long);
							goto fmt_valid;
						}

						goto fmt_invalid;
					}
					/* Size: long */
					if (_p[0] == 'l')
					{
						_p					+= 1;
						if (*_p != '\0' && strchr(intfmts, *_p) != nullptr)
						{
							va_arg(ap, long);
							goto fmt_valid;
						}
						goto fmt_invalid;
					}
	fmt_invalid:
					va_end(_cpy);
					goto err;
	fmt_valid:
					_l = (_p +1) -c;
					if (_l < sizeof(_format) - 2)
					{
						memcpy(_format, c, _l);
						_format[_l] 	 = '\0';
						newarg = sdscatvprintf(curarg, _format, _cpy);

						/* Update current position (note: outer blocks
						 * increment c twice so compensate here) */
						c = _p -1;
					}
					va_end(_cpy);
					break;
				
				}
			}
			
			if (newarg == nullptr)
				goto err;
			curarg = newarg;
			touched = 1;
			c++;
		}
	c++;
	}
	/* Add the last argument if needed */
	if (touched)
	{
		newargv = (char **)zrealloc(curargv, sizeof(char *) * (argc + 1));

		if (newargv == nullptr)
			goto err;

		curargv = newargv;
		curargv[argc++] 	= curarg;
		totlen += bulklen(sdslen(curarg));
	}
	else
	{
		sdsfree(curarg);
	}
	/* Clear curarg because it was put in curargv or was free'd. */
	curarg = nullptr;
	/* Add bytes needed to hold multi bulk count */
	totlen += 1 + intlen(argc) + 2;
	/* Build the command at protocol level */
	cmd 	= (char *)zmalloc(totlen + 1);
	if (cmd == nullptr)
		goto err;
	pos = sprintf(cmd, "*%d\r\n", argc);
	for (j = 0; j < argc; j++)
	{
		pos 	+= sprintf(cmd + pos, "$%zu\r\n", sdslen(curargv[j]));
		memcpy(cmd + pos, curargv[j], sdslen(curargv[j]));
		pos 	+= sdslen(curargv[j]);
		sdsfree(curargv[j]);
		cmd[pos++]	= '\r';
		cmd[pos++]	= '\n';
	}
	assert(pos == totlen);
	cmd[pos]			= '\0';
	zfree(curargv);
	*target 			= cmd;
	return totlen;
err:
	while (argc--)
		sdsfree(curargv[argc]);
	zfree(curargv);
	if (curarg != nullptr)
		sdsfree(curarg);
	if (cmd != nullptr)
		zfree(cmd);
	return - 1;
}

int32_t  xRedisContext::redisAppendFormattedCommand(const char *cmd, size_t len)
{
	if (redisAppendCommand(cmd, len) != REDIS_OK)
	{
		return REDIS_ERR;
	}
	return REDIS_OK;
}

int32_t redisFormatCommand(char **target, const char *format, ...)
{
    va_list ap;
    int32_t len;
    va_start(ap,format);
    len = redisvFormatCommand(target,format,ap);
    va_end(ap);
    if (len < 0)
        len = -1;
    return len;
}

void xRedisAsyncContext::redisvAsyncCommand(const RedisCallbackFn &fn,const std::any &privdata,const char *format, va_list ap)
{
	char *cmd;
	int32_t len;
	len = redisvFormatCommand(&cmd,format,ap);
	__redisAsyncCommand(fn,privdata,cmd,len);
}

void xRedisAsyncContext::redisAsyncCommand(const RedisCallbackFn &fn,const std::any &privdata,const char *format, ...)
{
	va_list ap;
	va_start(ap,format);
	redisvAsyncCommand(fn,privdata,format,ap);
	va_end(ap);
}

redisReply *xRedisContext::redisBlockForReply()
{
	redisReply *reply;
	if (flags & REDIS_BLOCK)
	{
		if (redisGetReply(&reply) != REDIS_OK)
			return nullptr;
		return reply;
	}
	return nullptr;
}

int32_t xRedisContext::redisAppendCommand(const char *cmd, size_t len)
{
	sender.append(cmd,len);
	return REDIS_OK;
}

int32_t xRedisContext::redisvAppendCommand(const char *format, va_list ap)
{
	char * cmd;
	int32_t len;

	len = redisvFormatCommand(&cmd, format, ap);
	if (len == -1)
	{
		redisSetError(REDIS_ERR_OOM,"Out of memory");
		return REDIS_ERR;
	}
	else if(len == -2)
	{	
		redisSetError(REDIS_ERR_OTHER,"Invalid format string");
       	return REDIS_ERR;
	}

	if (redisAppendCommand(cmd, len) != REDIS_OK)
	{
		zfree(cmd);
		return REDIS_ERR;
	}

	zfree(cmd);
	return REDIS_OK;
}


redisReply *xRedisContext::redisCommand(xBuffer *buffer)
{
	return redisBlockForReply();
}
redisReply *xRedisContext::redisCommand(const char *format, ...)
{
	va_list ap;
	redisReply *reply	= nullptr;
	va_start(ap, format);
	reply	= redisvCommand(format, ap);
	va_end(ap);
	return reply;
}

redisReply *xRedisContext::redisvCommand(const char *format, va_list ap)
{
	if (redisvAppendCommand(format, ap) != REDIS_OK)
		return nullptr;

	  return redisBlockForReply();
}

int32_t redisFormatCommandArgv(char **target, int32_t argc, const char **argv, const size_t *argvlen)
{
	char * cmd = nullptr; 					/* final command */
	int32_t pos;							/* position in final command */
	size_t	len;
	int32_t totlen, j;
	/* Calculate number of bytes needed for the command */
	totlen = 1 + intlen(argc) + 2;

	for (j = 0; j < argc; j++)
	{
		len = argvlen ? argvlen[j]: strlen(argv[j]);
		totlen += bulklen(len);
	}
	/* Build the command at protocol level */
	cmd = (char*)zmalloc(totlen + 1);
	if (cmd == nullptr)
		return - 1;
	pos = sprintf(cmd, "*%d\r\n", argc);

	for (j = 0; j < argc; j++)
	{
		len = argvlen ? argvlen[j]: strlen(argv[j]);
		pos += sprintf(cmd + pos, "$%zu\r\n", len);
		memcpy(cmd + pos, argv[j], len);
		pos += len;
		cmd[pos++] = '\r';
		cmd[pos++] = '\n';
	}

	assert(pos == totlen);
	cmd[pos] = '\0';
	*target = cmd;
	return totlen;
}

static uint32_t countDigits(uint64_t v)
{
	uint32_t result = 1;
	for (;;)
	{
		if (v < 10) return result;
		if (v < 100) return result + 1;
		if (v < 1000) return result + 2;
		if (v < 10000) return result + 3;
		v /= 10000U;
		result += 4;
	}
}

int32_t redisFormatSdsCommandArgv(sds *target, int32_t argc, const char **argv, const size_t *argvlen)
{
    sds cmd;
    uint64_t totlen;
    int32_t j;
    size_t len;

    /* Abort on a nullptr target */
    if (target == nullptr)
        return -1;

    /* Calculate our total size */
    totlen = 1+ countDigits(argc)+2;
    for (j = 0; j < argc; j++)
    {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        totlen += bulklen(len);
    }

    /* Use an SDS string for command construction */
    cmd = sdsempty();
    if (cmd == nullptr)
        return -1;

    /* We already know how much storage we need */
    cmd = sdsMakeRoomFor(cmd, totlen);
    if (cmd == nullptr)
        return -1;

    /* Construct command */
    cmd = sdscatfmt(cmd, "*%i\r\n", argc);
    for (j=0; j < argc; j++)
    {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        cmd = sdscatfmt(cmd, "$%u\r\n", len);
        cmd = sdscatlen(cmd, argv[j], len);
        cmd = sdscatlen(cmd, "\r\n", sizeof("\r\n")-1);
    }

    assert(sdslen(cmd)==totlen);

    *target = cmd;
    return totlen;
}

int32_t xRedisContext::redisAppendCommandArgv(int32_t argc, const char **argv, const size_t *argvlen)
{
	char * cmd;
	int32_t len;
	len = redisFormatCommandArgv(&cmd, argc, argv, argvlen);

	if (len == -1)
	{
		LOG_WARN<<"Out of memory";
		return REDIS_ERR;
	}

	if (redisAppendCommand(cmd, len) != REDIS_OK)
	{
		zfree(cmd);
		return REDIS_ERR;
	}

	zfree(cmd);
	return REDIS_OK;
}

redisReply *xRedisContext::redisCommandArgv(int32_t argc, const char * *argv, const size_t * argvlen)
{
	if (redisAppendCommandArgv(argc, argv, argvlen) != REDIS_OK)
		return nullptr;

	return redisBlockForReply();
}

int32_t xRedisContext::redisCheckSocketError()
{
	int32_t err = 0;
	socklen_t errlen = sizeof(err);

	if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1)
	{
	    redisSetError(REDIS_ERR_IO,"getsockopt(SO_ERROR)");
	    return REDIS_ERR;
	}

	if (err)
	{
	    errno = err;
	    redisSetError(REDIS_ERR_IO,nullptr);
	    return REDIS_ERR;
	}

	return REDIS_OK;
}

#define __MAX_MSEC (((LONG_MAX) - 999) / 1000)

 int32_t xRedisContext::redisContextWaitReady(int32_t msec)
{
	struct pollfd wfd[1];

	wfd[0].fd = fd;
	wfd[0].events = POLLOUT;

	if (errno == EINPROGRESS) 
	{
		int32_t res;
		if((res = ::poll(wfd,1,msec) )== -1)
		{
			redisSetError(REDIS_ERR_IO, "poll(2)");
			return REDIS_ERR;
		}
		else if(res == 0)
		{
			errno = ETIMEDOUT;
			redisSetError(REDIS_ERR_IO,nullptr);
			return REDIS_ERR;
		}

		 if(redisCheckSocketError() !=  REDIS_OK)
		 {
		 	return REDIS_ERR;
		 }

		  return REDIS_OK;
		  
	}

	redisSetError(REDIS_ERR_IO,nullptr);
	return REDIS_ERR;
	
}


static int32_t redisContextTimeoutMsec(const struct timeval *timeout,int32_t *result)
{
	int32_t msec = -1;

	/* Only use timeout when not nullptr. */
	if (timeout != nullptr) 
	{
		if (timeout->tv_usec > 1000000 || timeout->tv_sec > __MAX_MSEC) 
		{
		    *result = msec;
		    return REDIS_ERR;
		}

		msec = (timeout->tv_sec * 1000) + ((timeout->tv_usec + 999) / 1000);

		if (msec < 0 || msec > INT_MAX)
		{
		    msec = INT_MAX;
		}
	}

    *result = msec;
    return REDIS_OK;
}


int32_t xRedisContext::redisContextConnectTcp(const char *addr,int16_t port,const struct timeval *timeout)
{	
	int32_t rv;
	struct addrinfo hints,*servinfo;
	memset(&hints,0,sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	char _port[6];
	snprintf(_port, 6,"%d",port);

	if ((rv = getaddrinfo(addr,_port,&hints,&servinfo)) != 0)
	{
		redisSetError(REDIS_ERR_OTHER,gai_strerror(rv));
		return REDIS_ERR;
	}

	int32_t timeoutMsec = -1;

	if(timeout != nullptr)
	{
		if (redisContextTimeoutMsec(timeout,&timeoutMsec) != REDIS_OK)
		{
			redisSetError(REDIS_ERR_IO, "Invalid timeout specified");
			return REDIS_ERR;
		 }
	}

	int32_t blocking = (flags & REDIS_BLOCK);
	int32_t reuseaddr = (flags & REDIS_REUSEADDR);
	int32_t reuses = 0;

	xSocket socket;
	int32_t sockfd = socket.createSocket();
	if(sockfd == -1)
	{
		return REDIS_ERR;
	}
	
	fd = sockfd;

	socket.setSocketNonBlock(sockfd);

	if(socket.connect(sockfd, addr,port) == -1)
	{
	 	 if (errno == EHOSTUNREACH) 
		 {
		 	return REDIS_ERR;
	 	 }
		 else if(errno == EINPROGRESS && !blocking)
		 {
		 	
		 }
		 else if(errno == EADDRNOTAVAIL && reuseaddr) 
		 {
		 	 if (++reuses >= REDIS_CONNECT_RETRIES) 
			 {
			 	return REDIS_ERR;
		 	 }
			 else
			 {
			 	return REDIS_ERR;
			 }
		 }
		 else 
		 {
		 	 if(redisContextWaitReady(timeoutMsec)!= REDIS_OK)
		 	 {
		 	 	return REDIS_ERR;
		 	 }
		 }
	}

	socket.setSocketBlock(sockfd);
	socket.setTcpNoDelay(sockfd,true);

	ip = addr;
	p = port;

	return REDIS_OK;
}

RedisContextPtr redisConnect(const char *ip,int16_t port)
{
	RedisContextPtr c (new xRedisContext);
	c->setBlock();
	c->redisContextConnectTcp(ip,port,nullptr);
	return c;
}

RedisContextPtr redisConnectWithTimeout(const char *ip,int16_t port,const struct timeval tv)
{
	RedisContextPtr c (new xRedisContext);
	c->setBlock();
	c->redisContextConnectTcp(ip,port,&tv);
	return c;	
}

xHiredis::xHiredis(xEventLoop *loop,bool clusterMode)
:pool(loop),
clusterMode(true)
{

}

xHiredis::~xHiredis()
{

}

void xHiredis::clusterMoveConnCallBack(const TcpConnectionPtr &conn)
{
	auto callback = std::any_cast<RedisAsyncCallback>(conn->getContext());
	if(conn->connected())
	{
		RedisAsyncContextPtr ac(new xRedisAsyncContext(conn->intputBuffer(),conn));
		insertRedisMap(conn->getSockfd(),ac);

		{
			std::unique_lock<std::mutex> lk(ac->getMutex());
			ac->getCb().push_back(*callback);
		}
		
		conn->sendPipe(callback->data,callback->len);
	}
	else
	{
		eraseRedisMap(conn->getSockfd());
	}
}

void xHiredis::clusterAskConnCallBack(const TcpConnectionPtr &conn)
{
	auto callback = std::any_cast<RedisAsyncCallback>(conn->getContext());
	if(conn->connected())
	{
		RedisAsyncContextPtr ac (new xRedisAsyncContext(conn->intputBuffer(),conn));
		insertRedisMap(conn->getSockfd(),ac);
		conn->sendPipe("*1\r\n$6\r\nASKING\r\n");

		{
			std::unique_lock<std::mutex> lk(ac->getMutex());
			ac->getCb().push_back(*callback);
		}
		
		conn->sendPipe(callback->data,callback->len);
	}
	else
	{
		eraseRedisMap(conn->getSockfd());
	}
}

void xHiredis::clearTcpClient()
{
	std::unique_lock<std::mutex> lk(rtx);
	tcpClients.clear();
}

void xHiredis::eraseRedisMap(int32_t sockfd)
{
	std::unique_lock<std::mutex> lk(rtx);
	auto it = redisAsyncContexts.find(sockfd);
	assert(it != redisAsyncContexts.end());
	redisAsyncContexts.erase(it);
}

void xHiredis::insertRedisMap(int32_t sockfd,const RedisAsyncContextPtr &ac)
{
	std::unique_lock<std::mutex> lk(rtx);
	redisAsyncContexts.insert(std::make_pair(sockfd,ac));
}

void xHiredis::pushTcpClient(const TcpClientPtr &client)
{
	std::unique_lock<std::mutex> lk(rtx);
	tcpClients.push_back(client);
}

void xHiredis::redisReadCallBack(const TcpConnectionPtr &conn,xBuffer *buffer)
{
	RedisAsyncContextPtr redisPtr;
	{
		std::unique_lock <std::mutex> lck(rtx);
		auto it = redisAsyncContexts.find(conn->getSockfd());
		assert(it != redisAsyncContexts.end());
		redisPtr = it->second;
	}

	 redisAsyncCallback asyncCb;
	 redisReply *reply = nullptr;
	 int32_t status;
	 while((status = redisPtr->redisGetReply(&reply)) == REDIS_OK)
	 {
		 if(reply == nullptr)
		 {
			 break;
		 }

		 if(clusterMode && reply->type == REDIS_REPLY_ERROR && (!strncmp(reply->str,"MOVED",5) || (!strncmp(reply->str,"ASK",3))))
		 {	
			char *p = reply->str, *s;
			int32_t slot;
			s = strchr(p,' ');    
			p = strchr(s+1,' ');  
			*p = '\0';
			slot = atoi(s+1);
			s = strrchr(p+1,':'); 
			*s = '\0';
			
			std::string ip = p + 1;
			int32_t port = atoi(s+1);
			LOG_WARN<<"-> Redirected to slot "<< slot<<" located at "<<ip<<" "<<port;
			
			RedisAsyncCallback call;
			{
				std::unique_lock<std::mutex> lk(redisPtr->getMutex());
				assert(!redisPtr->getCb().empty());
				call = std::move(redisPtr->getCb().front());
				redisPtr->getCb().pop_front();
			}	

			TcpClientPtr client(new xTcpClient(pool.getNextLoop(),ip.c_str(),port,call));
			pushTcpClient(client);

			client->setMessageCallback(std::bind(&xHiredis::redisReadCallBack,this,std::placeholders::_1,std::placeholders::_2));
			if(!strncmp(reply->str,"MOVED",5))
			{
				client->setConnectionCallback(std::bind(&xHiredis::clusterMoveConnCallBack,this,std::placeholders::_1));
			}
			else if(!strncmp(reply->str,"ASK",3))
			{
				client->setConnectionCallback(std::bind(&xHiredis::clusterAskConnCallBack,this,std::placeholders::_1));
			}

			freeReply(reply);
			client->asyncConnect();
		 }
		 else
		 {
				 {
					 std::unique_lock<std::mutex> lk(redisPtr->getMutex());
					 assert(!redisPtr->getCb().empty());
					 asyncCb = std::move(redisPtr->getCb().front());
					 redisPtr->getCb().pop_front();
					 zfree(asyncCb.data);
				 }

				 if(asyncCb.cb.fn)
				 {
					 asyncCb.cb.fn(redisPtr,reply,asyncCb.cb.privdata);
				 }
				 
				freeReply(reply);
		 }
	 }
}

