#include "hiredis.h"

RedisCluster::RedisCluster() : proxyCount(-1), commandCount(-1), command(nullptr) {

}

RedisCluster::~RedisCluster() {

}

RedisReply::RedisReply() : str(nullptr), buffer(nullptr), integer(-1), type(-1), commandCount(-1) {

}

RedisReply::~RedisReply() {
	if (str != nullptr) {
		sdsfree(str);
	}

	if (buffer != nullptr) {
		sdsfree(buffer);
	}
}

RedisReader::RedisReader(Buffer *buffer) : buffer(buffer), pos(0), err(0), ridx(-1) {

}

RedisReader::RedisReader() : pos(0), err(0), ridx(-1) {
	buffer = &buf;
}

void RedisReader::redisReaderSetError(int32_t type, const char *str) {
	int32_t len;
	/* Clear input buffer on errors. */
	if (buffer != nullptr) {
		buffer->retrieveAll();
		pos = 0;
	}
	/* Reset task stack. */
	ridx = REDIS_ERR;
	/* Set error. */
	err = type;
	errstr = std::string(str, strlen(str));
}

static int32_t chrtos(char *buffer, int32_t size, char byte) {
	int32_t len = 0;
	switch (byte) {
	case '\\':
	case '"': {
		len = snprintf(buffer, size, "\"\\%c\"", byte);
		break;
	}
	case '\n':
		len = snprintf(buffer, size, "\"\\n\"");
		break;
	case '\r':
		len = snprintf(buffer, size, "\"\\r\"");
		break;
	case '\t':
		len = snprintf(buffer, size, "\"\\t\"");
		break;
	case '\a':
		len = snprintf(buffer, size, "\"\\a\"");
		break;
	case '\b':
		len = snprintf(buffer, size, "\"\\b\"");
		break;

	default:
		if (isprint(byte)) {
			len = snprintf(buffer, size, "\"%c\"", byte);
		}
		else {
			len = snprintf(buffer, size, "\"\\x%02x\"", (unsigned char)byte);
		}
		break;
	}
	return len;
}

void RedisReader::redisReaderSetErrorProtocolByte(char byte) {
	char cbuf[8], sbuf[128];
	chrtos(cbuf, sizeof(cbuf), byte);
	snprintf(sbuf, sizeof(sbuf), "Protocol error, got %s as reply type byte", cbuf);
	redisReaderSetError(REDIS_ERR_PROTOCOL, sbuf);
}

static int32_t intlen(int32_t i) {
	int32_t len = 0;
	if (i < 0) {
		len++;
		i = -i;
	}

	do {
		len++;
		i /= 10;
	} while (i);
	return len;
}

/* Create a reply object */
RedisReplyPtr createReplyObject(int32_t type) {
	RedisReplyPtr r(new RedisReply());
	assert(r != nullptr);

	r->type = type;
	return r;
}

/* Read a long long value starting at *s, under the assumption that it will be
 * terminated by \r\n. Ambiguously returns REDIS_ERR for unexpected input. */
int64_t RedisReader::readLongLong(const char *s) {
	int64_t v = 0;
	int32_t dec, mult = 1;
	char c;

	if (*s == '-') {
		mult = REDIS_ERR;
		s++;
	}
	else if (*s == '+') {
		mult = 1;
		s++;
	}

	while ((c = *(s++)) != '\r') {
		dec = c - '0';

		if (dec >= 0 && dec < 10) {
			v *= 10;
			v += dec;
		}
		else {
			/* Should not happen... */
			return REDIS_ERR;
		}
	}
	return mult * v;
}

/* Find pointer to \r\n. */
static const char *seekNewline(const char *s, int32_t len) {
	int32_t pos = 0;
	int32_t _len = len - 1;
	/* Position should be < lenREDIS_ERR because the character at "pos" should be
	 * followed by a \n. Note that strchr cannot be used because it doesn't
	 * allow to search a limited length and the buffer that is being searched
	 * might not have a trailing nullptr character. */
	while (pos < _len) {
		while (pos < _len && s[pos] != '\r') {
			pos++;
		}

		if (pos == _len) {
			/* Not found. */
			return nullptr;
		}
		else {
			if (s[pos + 1] == '\n') {
				/* Found. */
				return s + pos;
			}
			else {
				/* Continue searching. */
				pos++;
			}
		}
	}
	return nullptr;
}

const char *RedisReader::readLine(int32_t *_len) {
	const char *p = buffer->peek() + pos;
	const char *s = seekNewline(p, buffer->readableBytes() - pos);
	if (s != nullptr) {
		int32_t len = s - (buffer->peek() + pos);
		pos += len + 2;            /* skip \r\n */

		if (_len)
			*_len = len;
		return p;
	}
	return nullptr;
}

const char *RedisReader::readBytes(uint32_t bytes) {
	const char *p;
	if (buffer->readableBytes() - pos >= bytes) {
		p = buffer->peek() + pos;
		pos += bytes;
		return p;
	}
	return nullptr;
}

RedisReplyPtr createString(const RedisReadTask *task, const char *str, int32_t len) {
	RedisReplyPtr r, parent;
	r = createReplyObject(task->type);
	assert(r != nullptr);

	r->str = sdsnewlen(str, len);
	assert(task->type == REDIS_REPLY_ERROR || task->type == REDIS_REPLY_STATUS || task->type == REDIS_REPLY_STRING);
	if (task->parent) {
		parent = task->parent->weakObj.lock();
		assert(parent != nullptr);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element.push_back(r);
	}
	return r;
}

RedisReplyPtr createArray(const RedisReadTask *task, int32_t elements) {
	RedisReplyPtr r, parent;
	r = createReplyObject(REDIS_REPLY_ARRAY);
	assert(r != nullptr);

	if (elements > 0) {
		r->element.reserve(elements);
	}

	if (task->parent) {
		parent = task->parent->weakObj.lock();
		assert(parent != nullptr);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element.push_back(r);
	}
	return r;
}

RedisReplyPtr createInteger(const RedisReadTask *task, int64_t value) {
	RedisReplyPtr r, parent;
	r = createReplyObject(REDIS_REPLY_INTEGER);
	assert(r != nullptr);

	r->integer = value;
	if (task->parent) {
		parent = task->parent->weakObj.lock();
		assert(parent != nullptr);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element.push_back(r);
	}
	return r;
}

RedisReplyPtr createNil(const RedisReadTask *task) {
	RedisReplyPtr r, parent;
	r = createReplyObject(REDIS_REPLY_NIL);
	assert(r != nullptr);

	if (task->parent) {
		parent = task->parent->weakObj.lock();
		assert(parent != nullptr);
		assert(parent->type == REDIS_REPLY_ARRAY);
		parent->element.push_back(r);
	}
	return r;
}

/* Helper that calculates the bulk length given a certain string length. */
static int32_t bulklen(int32_t len) {
	return 1 + intlen(len) + 2 + len + 2;
}

static const char *nextArgument(const char *start, const char **str, int32_t *len) {
	const char *p = start;
	if (p[0] != '$') {
		p = strchr(p, '$');
		if (p == nullptr) {
			return nullptr;
		}
	}

	*len = (int32_t)strtol(p + 1, nullptr, 10);
	p = strchr(p, '\r');
	assert(p != nullptr);
	*str = p + 2;
	return p + 2 + (*len) + 2;
}

void RedisContext::redisSetError(int32_t type, const char *str) {
	int32_t len;
	err = type;
	if (str != nullptr) {
		errstr = std::string(str, strlen(str));
	}
	else {
		errstr = strerror(errno);
	}
}

void RedisReader::moveToNextTask() {
	RedisReadTask *cur, *prv;
	while (ridx >= 0) {
		/* Return a.s.a.p. when the stack is now empty. */
		if (ridx == 0) {
			ridx--;
			return;
		}

		cur = &(rstack[ridx]);
		prv = &(rstack[ridx - 1]);
		assert(prv->type == REDIS_REPLY_ARRAY);

		if (cur->idx == prv->elements - 1) {
			ridx--;
		}
		else {
			/* Reset the type because the next item can be anything */
			assert(cur->idx < prv->elements);
			cur->type = REDIS_ERR;
			cur->elements = REDIS_ERR;
			cur->idx++;
			return;
		}
	}
}

int32_t RedisReader::processLineItem() {
	RedisReadTask *cur = &(rstack[ridx]);
	RedisReplyPtr obj;
	const char *p;
	int32_t len;

	if ((p = readLine(&len)) != nullptr) {
		if (cur->type == REDIS_REPLY_INTEGER) {
			if (fn.createIntegerFuc) {
				obj = fn.createIntegerFuc(cur, readLongLong(p));
			}
		}
		else {
			/* Type will be error or status. */
			if (fn.createStringFuc) {
				obj = fn.createStringFuc(cur, p, len);
			}
		}

		if (obj == nullptr) {
			redisReaderSetErrorOOM();
			return REDIS_ERR;
		}

		/* Set reply if this is the root object. */
		if (ridx == 0) {
			reply = obj;
		}

		moveToNextTask();
		return REDIS_OK;
	}
	return REDIS_ERR;
}

int32_t RedisReader::processBulkItem() {
	RedisReadTask *cur = &(rstack[ridx]);
	RedisReplyPtr obj;
	const char *p;
	const char *s;
	int32_t len;
	uint32_t bytelen;
	int32_t success = 0;

	p = buffer->peek() + pos;
	s = seekNewline(p, buffer->readableBytes() - pos);

	if (s != nullptr) {
		p = buffer->peek() + pos;
		bytelen = s - (buffer->peek() + pos) + 2; /* include \r\n */
		len = readLongLong(p);

		if (len < 0) {
			/* The nil object can always be created. */
			if (fn.createNilFuc) {
				obj = fn.createNilFuc(cur);
			}
			success = 1;
		}
		else {
			/* Only continue when the buffer contains the entire bulk item. */
			bytelen += len + 2;        /* include \r\n */
			if (pos + bytelen <= buffer->readableBytes()) {
				if (fn.createStringFuc) {
					obj = fn.createStringFuc(cur, s + 2, len);
				}
				success = 1;
			}
		}

		/* Proceed when obj was created. */
		if (success) {
			if (obj == nullptr) {
				redisReaderSetErrorOOM();
				return REDIS_ERR;
			}

			pos += bytelen;
			/* Set reply if this is the root object. */
			if (ridx == 0) {
				reply = obj;
			}

			moveToNextTask();
			return REDIS_OK;
		}
	}
	return REDIS_ERR;
}

int32_t RedisReader::processMultiBulkItem() {
	RedisReadTask *cur = &(rstack[ridx]);
	RedisReplyPtr obj;
	const char *p;
	int32_t elements;
	int32_t root = 0;

	/* Set error for nested multi bulks with depth > 7 */
	if (ridx == 8) {
		redisReaderSetError(REDIS_ERR_PROTOCOL, "No support for nested multi bulk replies with depth > 7");
		return REDIS_ERR;
	}

	if ((p = readLine(nullptr)) != nullptr) {
		elements = readLongLong(p);
		root = (ridx == 0);
		if (elements == REDIS_ERR) {
			if (fn.createNilFuc) {
				obj = fn.createNilFuc(cur);
			}

			if (obj == nullptr) {
				redisReaderSetErrorOOM();
				return REDIS_ERR;
			}
			moveToNextTask();
		}
		else {
			if (fn.createArrayFuc) {
				obj = fn.createArrayFuc(cur, elements);
			}

			if (obj == nullptr) {
				redisReaderSetErrorOOM();
				return REDIS_ERR;
			}

			/* Modify task stack when there are more than 0 elements. */
			if (elements > 0) {
				cur->elements = elements;
				cur->weakObj = obj;
				ridx++;
				rstack[ridx].type = REDIS_ERR;
				rstack[ridx].elements = REDIS_ERR;
				rstack[ridx].idx = 0;
				rstack[ridx].parent = cur;
				rstack[ridx].privdata = privdata;
			}
			else {
				moveToNextTask();
			}
		}

		/* Set reply if this is the root object. */
		if (root) {
			reply = obj;
		}
		return REDIS_OK;
	}
	return REDIS_ERR;
}

int32_t RedisReader::processItem() {
	RedisReadTask *cur = &(rstack[ridx]);
	const char *p;

	/* check if we need to read type */
	if (cur->type < 0) {
		if ((p = readBytes(1)) != nullptr) {
			switch (p[0]) {
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
				assert(false);
				redisReaderSetErrorProtocolByte(*((char *)p));
				return REDIS_ERR;
			}
		}
		else {
			/* could not consume 1 byte */
			return REDIS_ERR;
		}
	}

	/* process typed item */
	switch (cur->type) {
	case REDIS_REPLY_ERROR:
	case REDIS_REPLY_STATUS:
	case REDIS_REPLY_INTEGER:
		return processLineItem();
	case REDIS_REPLY_STRING:
		return processBulkItem();
	case REDIS_REPLY_ARRAY:
		return processMultiBulkItem();
	default:
		assert(false);
		return REDIS_ERR; /* Avoid warning. */
	}
}

void RedisReader::redisReaderSetErrorOOM() {
	redisReaderSetError(REDIS_ERR_OOM, "Out of memory");
}

int32_t RedisReader::redisReaderGetReply(RedisReplyPtr &reply) {
	if (err) {
		return REDIS_ERR;
	}

	if (buffer->readableBytes() == 0) {
		return REDIS_OK;
	}

	if (ridx == REDIS_ERR) {
		rstack[0].type = REDIS_ERR;
		rstack[0].elements = REDIS_ERR;
		rstack[0].idx = REDIS_ERR;
		rstack[0].parent = nullptr;
		rstack[0].privdata = privdata;
		rstack[0].weakObj.reset();
		ridx = 0;
	}

	while (ridx >= 0) {
		if (processItem() != REDIS_OK) {
			break;
		}
	}

	/* Emit a reply when there is one. */
	if (ridx == REDIS_ERR) {
		reply = this->reply;
		this->reply.reset();
		reply->buffer = sdsnewlen(buffer->peek(), pos);
		assert(pos <= buffer->readableBytes());
		buffer->retrieve(pos);
		pos = 0;
	}
	return REDIS_OK;
}

RedisAsyncCallback::RedisAsyncCallback() : data(nullptr), len(0), type(false) {

}

RedisAsyncCallback::~RedisAsyncCallback() {
	if (data != nullptr) {
		if (type) {
			sdsfree(data);
		}
		else {
			zfree(data);
		}
	}
}

RedisContext::RedisContext() : reader(new RedisReader()) {
	clear();
}

RedisContext::RedisContext(Buffer *buffer, int32_t sockfd) : reader(new RedisReader(buffer)) {
	clear();
}

void RedisContext::close() {
	Socket::close(fd);
}

void RedisContext::clear() {
	err = 0;
	errstr.clear();
	fd = REDIS_ERR;
}

RedisContext::~RedisContext() {
	Socket::close(fd);
}

/* Write the output buffer to the socket.
 *
 * Returns REDIS_OK when the buffer is empty, or (a part of) the buffer was
 * successfully written to the socket. When the buffer is empty after the
 * write operation, "done" is set to 1 (if given).
 *
 * Returns REDIS_ERR if an error occurred trying to write and sets
 * c->errstr to hold the appropriate error string.
 */

int32_t RedisContext::redisBufferWrite(int32_t *done) {
	int32_t nwritten;
	if (err) {
		return REDIS_ERR;
	}

	if (sender.readableBytes() > 0) {
		nwritten = Socket::write(fd, sender.peek(), sender.readableBytes());
		if (nwritten < 0) {
			if ((errno == EAGAIN && !(flags == REDIS_BLOCK)) || (errno == EINTR)) {
				/* Try again later */
			}
			else {
				redisSetError(REDIS_ERR_IO, nullptr);
				return REDIS_ERR;
			}
		}
		else if (nwritten > 0) {
			sender.retrieve(nwritten);
		}
	}

	if (done != nullptr) {
		*done = (sender.readableBytes() == 0);
	}
	return REDIS_OK;
}

/* Use this function to handle a read event on the descriptor. It will try
 * and read some bytes from the socket and feed them to the reply parser.
 *
 * After this function is called, you may use redisContextReadReply to
 * see if there is a reply available. */

int32_t RedisContext::redisBufferRead() {
	int32_t savedErrno = 0;
	/* Return early when the redisContext has seen an error. */
	if (err) {
		return REDIS_ERR;
	}

	ssize_t n = reader->buffer->readFd(fd, &savedErrno);
	if (n > 0) {

	}
	else if (n == 0) {
		redisSetError(REDIS_ERR_EOF, "Server closed the connection");
		return REDIS_ERR;
	}
	else {
		if ((savedErrno == EAGAIN && !(flags == REDIS_BLOCK)) || (savedErrno == EINTR)) {
			/* Try again later */
		}
		else {
			redisSetError(REDIS_ERR_IO, nullptr);
			return REDIS_ERR;
		}
	}
	return REDIS_OK;
}

int32_t RedisContext::redisGetReply(RedisReplyPtr &reply) {
	int32_t wdone = 0;
	RedisReplyPtr aux;
	/* Try to read pending replies */
	if (reader->redisReaderGetReply(aux) == REDIS_ERR) {
		redisSetError(reader->err, reader->errstr.c_str());
		return REDIS_ERR;
	}

	/* For the blocking redisContext, flush output buffer and read reply */
	if (aux == nullptr && flags == REDIS_BLOCK) {
		/* Write until done */
		do {
			if (redisBufferWrite(&wdone) == REDIS_ERR) {
				return REDIS_ERR;
			}

		} while (!wdone);


		/* Read until there is a reply */
		do {
			if (redisBufferRead() == REDIS_ERR) {
				return REDIS_ERR;
			}

			if (reader->redisReaderGetReply(aux) == REDIS_ERR) {
				redisSetError(reader->err, reader->errstr.c_str());
				return REDIS_ERR;
			}
		} while (aux == nullptr);
	}

	reply = aux;
	return REDIS_OK;
}

int32_t RedisContext::redisAppendCommand(const char *format, ...) {
	va_list ap;
	int32_t ret;

	va_start(ap, format);
	ret = redisvAppendCommand(format, ap);
	va_end(ap);
	return ret;
}

int32_t RedisAsyncContext::proxyAsyncCommand(const RedisAsyncCallbackPtr &asyncCallback) {
	TcpConnectionPtr conn = weakRedisConn.lock();
	assert(conn != nullptr);
	conn->getLoop()->assertInLoopThread();
	if (redisContext->flags == REDIS_DISCONNECTING) {
		return REDIS_ERR;
	}

	int32_t pvariant, hasnext;
	const char *cstr, *astr;
	int32_t clen, alen;
	const char *p;
	sds sname;
	int32_t ret;

	int32_t len = asyncCallback->len;
	const char *cmd = asyncCallback->data;

	/* Find out which command will be appended. */
	p = nextArgument(cmd, &cstr, &clen);
	assert(p != nullptr);
	hasnext = (p[0] == '$');
	pvariant = (tolower(cstr[0]) == 'p') ? 1 : 0;
	cstr += pvariant;
	clen -= pvariant;

	if (hasnext && MEMCMP(cstr, "subscribe\r\n", 11) == 0) {
		redisContext->flags = REDIS_SUBSCRIBED;
		/* Add every channel/pattern to the list of subscription callbacks. */
		while ((p = nextArgument(p, &astr, &alen)) != nullptr) {
			sname = sdsnewlen(astr, alen);
			if (pvariant) {
				subCb.patternCb.insert(std::make_pair(createObject(REDIS_STRING, sname), asyncCallback));
			}
			else {
				subCb.channelCb.insert(std::make_pair(createObject(REDIS_STRING, sname), asyncCallback));
			}
		}
	}
	else if (MEMCMP(cstr, "unsubscribe\r\n", 13) == 0) {
		/* It is only useful to call (P)UNSUBSCRIBE when the context is
		* subscribed to one or more channels or patterns. */
		if (redisContext->flags != REDIS_SUBSCRIBED) {
			return REDIS_ERR;
		}
		/* (P)UNSUBSCRIBE does not have its own response: every channel or
		* pattern that is unsubscribed will receive a message. This means we
		* should not append a callback function for this command. */
	}
	else if (MEMCMP(cstr, "monitor\r\n", 9) == 0) {
		/* Set monitor flag and push callback */
		redisContext->flags = REDIS_MONITORING;
		repliesCb.push_back(asyncCallback);
	}
	else {
		if (redisContext->flags == REDIS_SUBSCRIBED) {
			/* This will likely result in an error reply, but it needs to be
			* received and passed to the callback. */
			subCb.invalidCb.push_back(asyncCallback);
		}
		else {
			repliesCb.push_back(asyncCallback);
		}
	}

	conn->sendPipe();
	return REDIS_OK;
}

int32_t RedisAsyncContext::__redisAsyncCommand(const RedisAsyncCallbackPtr &asyncCallback) {
	TcpConnectionPtr conn = weakRedisConn.lock();
	assert(conn != nullptr);
	conn->getLoop()->assertInLoopThread();
	if (redisContext->flags == REDIS_DISCONNECTING) {
		return REDIS_ERR;
	}

	int32_t pvariant, hasnext;
	const char *cstr, *astr;
	int32_t clen, alen;
	const char *p;
	sds sname;
	int32_t ret;

	int32_t len = asyncCallback->len;
	const char *cmd = asyncCallback->data;

	/* Find out which command will be appended. */
	p = nextArgument(cmd, &cstr, &clen);
	if (p != nullptr) {
		hasnext = (p[0] == '$');
		pvariant = (tolower(cstr[0]) == 'p') ? 1 : 0;
		cstr += pvariant;
		clen -= pvariant;

		if (hasnext && MEMCMP(cstr, "subscribe\r\n", 11) == 0) {
			redisContext->flags = REDIS_SUBSCRIBED;
			/* Add every channel/pattern to the list of subscription callbacks. */
			while ((p = nextArgument(p, &astr, &alen)) != nullptr) {
				sname = sdsnewlen(astr, alen);
				if (pvariant) {
					subCb.patternCb.insert(std::make_pair(createObject(REDIS_STRING, sname), asyncCallback));
				}
				else {
					subCb.channelCb.insert(std::make_pair(createObject(REDIS_STRING, sname), asyncCallback));
				}
			}
		}
		else if (MEMCMP(cstr, "unsubscribe\r\n", 13) == 0) {
			/* It is only useful to call (P)UNSUBSCRIBE when the context is
			* subscribed to one or more channels or patterns. */
			if (redisContext->flags != REDIS_SUBSCRIBED) {
				return REDIS_ERR;
			}

			/* (P)UNSUBSCRIBE does not have its own response: every channel or
			* pattern that is unsubscribed will receive a message. This means we
			* should not append a callback function for this command. */
		}
		else if (MEMCMP(cstr, "monitor\r\n", 9) == 0) {
			/* Set monitor flag and push callback */
			redisContext->flags = REDIS_MONITORING;
			repliesCb.push_back(asyncCallback);
		}
		else {
			if (redisContext->flags == REDIS_SUBSCRIBED) {
				/* This will likely result in an error reply, but it needs to be
				* received and passed to the callback. */
				subCb.invalidCb.push_back(asyncCallback);
			}
			else {
				repliesCb.push_back(asyncCallback);
			}
		}
	}
	else {
		repliesCb.push_back(asyncCallback);
	}

	conn->outputBuffer()->append(cmd, len);
	conn->sendPipe();
	return REDIS_OK;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
* Implementation borrowed from link in redis/src/util.c:string2ll(). */
static uint32_t countDigits(uint64_t v) {
	uint32_t result = 1;
	for (;;) {
		if (v < 10) return result;
		if (v < 100) return result + 1;
		if (v < 1000) return result + 2;
		if (v < 10000) return result + 3;
		v /= 10000U;
		result += 4;
	}
}

int32_t redisvFormatCommand(char **target, const char *format, va_list ap) {
	const char *c = format;
	char *cmd = nullptr; /* final command */
	int32_t pos; /* position in final command */
	sds curarg, newarg; /* current argument */
	int32_t touched = 0; /* was the current argument touched? */
	char **curargv = nullptr, **newargv = nullptr;
	int32_t argc = 0;
	int32_t totlen = 0;
	int32_t errorType = 0; /* 0 = no error; REDIS_ERR = memory error; -2 = format error */
	int32_t j;

	/* Abort if there is not target to set */
	if (target == nullptr)
		return REDIS_ERR;

	/* Build the command string accordingly to protocol */
	curarg = sdsempty();
	if (curarg == nullptr)
		return REDIS_ERR;

	while (*c != '\0') {
		if (*c != '%' || c[1] == '\0') {
			if (*c == ' ') {
				if (touched) {
					newargv = (char **)zrealloc(curargv, sizeof(char *) * (argc + 1));
					if (newargv == nullptr) goto memoryErr;
					curargv = newargv;
					curargv[argc++] = curarg;
					totlen += bulklen(sdslen(curarg));

					/* curarg is put in argv so it can be overwritten. */
					curarg = sdsempty();
					if (curarg == nullptr) goto memoryErr;
					touched = 0;
				}
			}
			else {
				newarg = sdscatlen(curarg, c, 1);
				if (newarg == nullptr) goto memoryErr;
				curarg = newarg;
				touched = 1;
			}
		}
		else {
			char *arg;
			int32_t size;

			/* Set newarg so it can be checked even if it is not touched. */
			newarg = curarg;

			switch (c[1]) {
			case 's':
				arg = va_arg(ap, char *);
				size = strlen(arg);
				if (size > 0)
					newarg = sdscatlen(curarg, arg, size);
				break;
			case 'b':
				arg = va_arg(ap, char *);
				size = va_arg(ap, int32_t);
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
				static const char flags[] = "#0-+ ";
				char _format[16];
				const char *_p = c + 1;
				int32_t _l = 0;
				va_list _cpy;

				/* Flags */
				while (*_p != '\0' && strchr(flags, *_p) != nullptr) _p++;

				/* Field width */
				while (*_p != '\0' && isdigit(*_p)) _p++;

				/* Precision */
				if (*_p == '.') {
					_p++;
					while (*_p != '\0' && isdigit(*_p)) _p++;
				}

				/* Copy va_list before consuming with va_arg */
				va_copy(_cpy, ap);

				/* Integer conversion (without modifiers) */
				if (strchr(intfmts, *_p) != nullptr) {
					va_arg(ap, int32_t);
					goto fmtValid;
				}

				/* Double conversion (without modifiers) */
				if (strchr("eEfFgGaA", *_p) != nullptr) {
					va_arg(ap, double);
					goto fmtValid;
				}

				/* Size: char */
				if (_p[0] == 'h' && _p[1] == 'h') {
					_p += 2;
					if (*_p != '\0' && strchr(intfmts, *_p) != nullptr) {
						va_arg(ap, int32_t); /* char gets promoted to int32_t */
						goto fmtValid;
					}
					goto fmtInvalid;
				}

				/* Size: short */
				if (_p[0] == 'h') {
					_p += 1;
					if (*_p != '\0' && strchr(intfmts, *_p) != nullptr) {
						va_arg(ap, int32_t); /* short gets promoted to int32_t */
						goto fmtValid;
					}
					goto fmtInvalid;
				}

				/* Size: long long */
				if (_p[0] == 'l' && _p[1] == 'l') {
					_p += 2;
					if (*_p != '\0' && strchr(intfmts, *_p) != nullptr) {
						va_arg(ap, long long);
						goto fmtValid;
					}
					goto fmtInvalid;
				}

				/* Size: long */
				if (_p[0] == 'l') {
					_p += 1;
					if (*_p != '\0' && strchr(intfmts, *_p) != nullptr) {
						va_arg(ap, long);
						goto fmtValid;
					}
					goto fmtInvalid;
				}

			fmtInvalid:
				va_end(_cpy);
				goto formatErr;

			fmtValid:
				_l = (_p + 1) - c;
				if (_l < sizeof(_format) - 2) {
					memcpy(_format, c, _l);
					_format[_l] = '\0';
					newarg = sdscatvprintf(curarg, _format, _cpy);

					/* Update current position (note: outer blocks
					 * increment c twice so compensate here) */
					c = _p - 1;
				}

				va_end(_cpy);
				break;
			}
			}

			if (newarg == nullptr) goto memoryErr;
			curarg = newarg;

			touched = 1;
			c++;
		}
		c++;
	}

	/* Add the last argument if needed */
	if (touched) {
		newargv = (char **)zrealloc(curargv, sizeof(char *) * (argc + 1));
		if (newargv == nullptr) goto memoryErr;
		curargv = newargv;
		curargv[argc++] = curarg;
		totlen += bulklen(sdslen(curarg));
	}
	else {
		sdsfree(curarg);
	}

	/* Clear curarg because it was put in curargv or was zfree'd. */
	curarg = nullptr;

	/* Add bytes needed to hold multi bulk count */
	totlen += 1 + countDigits(argc) + 2;

	/* Build the command at protocol level */
	cmd = (char *)zmalloc(totlen + 1);
	if (cmd == nullptr) goto memoryErr;

	pos = sprintf(cmd, "*%d\r\n", argc);
	for (j = 0; j < argc; j++) {
		pos += sprintf(cmd + pos, "$%zu\r\n", sdslen(curargv[j]));
		memcpy(cmd + pos, curargv[j], sdslen(curargv[j]));
		pos += sdslen(curargv[j]);
		sdsfree(curargv[j]);
		cmd[pos++] = '\r';
		cmd[pos++] = '\n';
	}

	assert(pos == totlen);
	cmd[pos] = '\0';

	zfree(curargv);
	*target = cmd;
	return totlen;

formatErr:
	errorType = -2;
	goto cleanup;

memoryErr:
	errorType = REDIS_ERR;
	goto cleanup;

cleanup:
	if (curargv) {
		while (argc--)
			sdsfree(curargv[argc]);
		zfree(curargv);
	}

	sdsfree(curarg);

	/* No need to check cmd since it is the last statement that can fail,
	 * but do it anyway to be as defensive as possible. */
	if (cmd != nullptr)
		zfree(cmd);

	return errorType;
}

/* Format a command according to the Redis protocol. This function
 * takes a format similar to printf:
 *
 * %s represents a C null terminated string you want to interpolate
 * %b represents a binary safe string
 *
 * When using %b you need to provide both the pointer to the string
 * and the length in bytes as a int32_t. Examples:
 *
 * len = redisFormatCommand(target, "GET %s", mykey);
 * len = redisFormatCommand(target, "SET %s %b", mykey, myval, myvallen);
 */

int32_t redisFormatCommand(char **target, const char *format, ...) {
	va_list ap;
	int32_t len;
	va_start(ap, format);
	len = redisvFormatCommand(target, format, ap);
	va_end(ap);
	if (len < 0) {
		len = REDIS_ERR;
	}
	return len;
}

RedisReplyPtr RedisContext::redisBlockForReply() {
	RedisReplyPtr reply;
	if (redisGetReply(reply) != REDIS_OK) {
		return nullptr;
	}
	return reply;
}

int32_t RedisContext::redisvAppendCommand(const char *format, va_list ap) {
	char *cmd;
	int32_t len;

	len = redisvFormatCommand(&cmd, format, ap);
	if (len == REDIS_ERR) {
		redisSetError(REDIS_ERR_OOM, "Out of memory");
		return REDIS_ERR;
	}
	else if (len == -2) {
		redisSetError(REDIS_ERR_OTHER, "Invalid format string");
		return REDIS_ERR;
	}

	redisAppendCommand(cmd, len);
	zfree(cmd);
	return REDIS_OK;
}

RedisReplyPtr RedisContext::redisCommand(const char *format, ...) {
	va_list ap;
	RedisReplyPtr reply = nullptr;
	va_start(ap, format);
	reply = redisvCommand(format, ap);
	va_end(ap);
	return reply;
}

RedisReplyPtr RedisContext::redisvCommand(const char *format, va_list ap) {
	if (redisvAppendCommand(format, ap) != REDIS_OK) {
		return nullptr;
	}
	return redisBlockForReply();
}

/* Format a command according to the Redis protocol. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to nullptr, strlen will be used to compute the
 * argument lengths.
 */
int32_t redisFormatCommandArgv(char **target, int32_t argc, const char **argv, const int32_t *argvlen) {
	char *cmd = nullptr;                    /* final command */
	int32_t pos;                            /* position in final command */
	int32_t len;
	int32_t totlen, j;
	/* Calculate number of bytes needed for the command */
	totlen = 1 + intlen(argc) + 2;

	for (j = 0; j < argc; j++) {
		len = argvlen ? argvlen[j] : strlen(argv[j]);
		totlen += bulklen(len);
	}
	/* Build the command at protocol level */
	cmd = (char *)zmalloc(totlen + 1);
	if (cmd == nullptr) {
		return REDIS_ERR;
	}

	pos = sprintf(cmd, "*%d\r\n", argc);

	for (j = 0; j < argc; j++) {
		len = argvlen ? argvlen[j] : strlen(argv[j]);
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

/* Format a command according to the Redis protocol using an sds string and
 * sdscatfmt for the processing of arguments. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to nullptr, strlen will be used to compute the
 * argument lengths.
 */
int32_t redisFormatSdsCommandArgv(sds *target, int32_t argc, const char **argv, const int32_t *argvlen) {
	sds cmd;
	uint64_t totlen;
	int32_t j;
	int32_t len;

	/* Abort on a nullptr target */
	if (target == nullptr) {
		return REDIS_ERR;
	}

	/* Calculate our total size */
	totlen = 1 + countDigits(argc) + 2;
	for (j = 0; j < argc; j++) {
		len = argvlen ? argvlen[j] : strlen(argv[j]);
		totlen += bulklen(len);
	}
	/* Use an SDS string for command construction */
	cmd = sdsempty();
	if (cmd == nullptr) {
		return REDIS_ERR;
	}

	/* We already know how much storage we need */
	cmd = sdsMakeRoomFor(cmd, totlen);
	if (cmd == nullptr) {
		return REDIS_ERR;
	}

	/* Construct command */
	cmd = sdscatfmt(cmd, "*%i\r\n", argc);
	for (j = 0; j < argc; j++) {
		len = argvlen ? argvlen[j] : strlen(argv[j]);
		cmd = sdscatfmt(cmd, "$%u\r\n", len);
		cmd = sdscatlen(cmd, argv[j], len);
		cmd = sdscatlen(cmd, "\r\n", sizeof("\r\n") - 1);
	}

	assert(sdslen(cmd) == totlen);
	*target = cmd;
	return totlen;
}

void RedisContext::redisAppendCommand(const char *cmd, int32_t len) {
	sender.append(cmd, len);
}

void RedisContext::redisAppendFormattedCommand(const char *cmd, int32_t len) {
	redisAppendCommand(cmd, len);
}

int32_t RedisContext::redisAppendCommandArgv(int32_t argc, const char **argv, const int32_t *argvlen) {
	char *cmd;
	int32_t len;
	len = redisFormatCommandArgv(&cmd, argc, argv, argvlen);

	if (len == REDIS_ERR) {
		redisSetError(REDIS_ERR_OOM, "Out of memory");
		return REDIS_ERR;
	}

	redisAppendCommand(cmd, len);
	zfree(cmd);
	return REDIS_OK;
}

RedisReplyPtr RedisContext::redisCommandArgv(int32_t argc, const char **argv, const int32_t *argvlen) {
	if (redisAppendCommandArgv(argc, argv, argvlen) != REDIS_OK) {
		return nullptr;
	}
	return redisBlockForReply();
}

int32_t RedisContext::redisCheckSocketError() {
	int32_t err = 0;
	socklen_t errlen = sizeof(err);

	if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, (char *)&err, &errlen) == REDIS_ERR) {
		redisSetError(REDIS_ERR_IO, "getsockopt(SO_ERROR)");
		return REDIS_ERR;
	}

	if (err) {
		errno = err;
		redisSetError(REDIS_ERR_IO, nullptr);
		return REDIS_ERR;
	}
	return REDIS_OK;
}

#define __MAX_MSEC (((LONG_MAX) - 999) / 1000)

int32_t RedisContext::redisContextWaitReady(int32_t msec) {
	struct pollfd wfd[1];

	wfd[0].fd = fd;
	wfd[0].events = POLLOUT;

	if (errno == EINPROGRESS) {
		int32_t res;
#ifdef _WIN64
		if ((res = ::WSAPoll(wfd, 1, msec)) == REDIS_ERR)
#else
		if ((res = ::poll(wfd, 1, msec)) == REDIS_ERR)
#endif
		{
			redisSetError(REDIS_ERR_IO, "poll(2)");
			return REDIS_ERR;
		}
		else if (res == 0) {
			errno = ETIMEDOUT;
			redisSetError(REDIS_ERR_IO, nullptr);
			return REDIS_ERR;
		}

		if (redisCheckSocketError() != REDIS_OK) {
			return REDIS_ERR;
		}
		return REDIS_OK;
	}

	redisSetError(REDIS_ERR_IO, nullptr);
	return REDIS_ERR;
}

static int32_t redisContextTimeoutMsec(const struct timeval *timeout, int32_t *result) {
	int32_t msec = REDIS_ERR;

	/* Only use timeout when not nullptr. */
	if (timeout != nullptr) {
		if (timeout->tv_usec > 1000000 || timeout->tv_sec > __MAX_MSEC) {
			*result = msec;
			return REDIS_ERR;
		}

		msec = (timeout->tv_sec * 1000) + ((timeout->tv_usec + 999) / 1000);
		if (msec < 0 || msec > INT_MAX) {
			msec = INT_MAX;
		}
	}

	*result = msec;
	return REDIS_OK;
}

int32_t RedisContext::redisContextConnectUnix(const char *path, const struct timeval *timeout) {
	return REDIS_OK;
}

int32_t RedisContext::redisReconnect() {
	LOG_INFO << "Reconnect redis info " << ip << " " << port;
	Socket::close(fd);
	fd = REDIS_ERR;
	err = 0;
	sender.retrieveAll();
	reader.reset(new RedisReader());

	int32_t sockfd = Socket::createSocket();
	if (sockfd == REDIS_ERR) {
		redisSetError(REDIS_ERR_OTHER, nullptr);
		return REDIS_ERR;
	}

	int32_t ret = Socket::connect(sockfd, ip.c_str(), port);
	if (ret == REDIS_ERR) {
		Socket::close(sockfd);
		redisSetError(REDIS_ERR_IO, nullptr);
		return REDIS_ERR;
	}

	fd = sockfd;
	flags = REDIS_BLOCK;
	return REDIS_OK;
}

int32_t RedisContext::redisContextConnectTcp(const char *addr, int16_t p, const struct timeval *timeout) {
	ip = addr;
	port = p;

	int32_t sockfd = Socket::createSocket();
	if (sockfd == REDIS_ERR) {
		redisSetError(REDIS_ERR_OTHER, nullptr);
		return REDIS_ERR;
	}

	int32_t ret = Socket::connect(sockfd, ip.c_str(), port);
	if (ret == REDIS_ERR) {
		Socket::close(sockfd);
		redisSetError(REDIS_ERR_IO, nullptr);
		return REDIS_ERR;
	}

	fd = sockfd;
	flags = REDIS_BLOCK;
	return REDIS_OK;
}

SubCallback::SubCallback() {

}

SubCallback::~SubCallback() {

}

RedisAsyncContext::RedisAsyncContext(Buffer *buffer, const TcpConnectionPtr &conn) : redisContext(
	new RedisContext(buffer, conn->getSockfd())), weakRedisConn(conn), err(0) {

}

RedisAsyncContext::~RedisAsyncContext() {
	//repliesCb.clear();
}

std::function<void()> RedisAsyncContext::getRedisAsyncCommand(const RedisCallbackFn &fn,
	const std::any &privdata, const char *format, ...) {
	va_list ap;
	va_start(ap, format);
	auto status = getRedisvAsyncCommand(fn, privdata, format, ap);
	va_end(ap);
	return status;
}

std::function<void()> RedisAsyncContext::getRedisvAsyncCommand(const RedisCallbackFn &fn,
	const std::any &privdata, const char *format, va_list ap) {
	char *cmd;
	int32_t len;
	len = redisvFormatCommand(&cmd, format, ap);
	if (len < 0) {
		return nullptr;
	}

	if (redisContext->flags == REDIS_DISCONNECTING) {
		zfree(cmd);
		return nullptr;
	}

	RedisCallback cb;
	cb.fn = std::move(fn);
	cb.privdata = privdata;

	RedisAsyncCallbackPtr asyncCallback(new RedisAsyncCallback());
	asyncCallback->data = cmd;
	asyncCallback->len = len;
	asyncCallback->cb = std::move(cb);
	
	return std::bind(&RedisAsyncContext::__redisAsyncCommand, shared_from_this(), asyncCallback);
}

int32_t RedisAsyncContext::processCommand(const RedisCallbackFn &fn, const std::any &privdata,
	const std::vector <RedisObjectPtr> &commands) {
	TcpConnectionPtr conn = weakRedisConn.lock();
	assert(conn != nullptr);
	Buffer *buffer = conn->outputBuffer();
	int32_t readableBytes = buffer->readableBytes();
	int32_t writableBytes = 0;

	int32_t len, j;
	char buf[32];
	buf[0] = '*';
	len = 1 + ll2string(buf + 1, sizeof(buf) - 1, commands.size());
	buf[len++] = '\r';
	buf[len++] = '\n';
	buffer->append(buf, len);
	writableBytes += len;

	for (int i = 0; i < commands.size(); i++) {
		char buf[32];
		buf[0] = '$';
		len = 1 + ll2string(buf + 1, sizeof(buf) - 1, sdslen(commands[i]->ptr));
		buf[len++] = '\r';
		buf[len++] = '\n';
		buffer->append(buf, len);
		writableBytes += len;

		buffer->append(commands[i]->ptr, sdslen(commands[i]->ptr));
		writableBytes += sdslen(commands[i]->ptr);

		buffer->append("\r\n", 2);
		writableBytes += 2;
	}

	char *data = (char *)zmalloc(writableBytes);
	memcpy(data, buffer->peek() + readableBytes, writableBytes);
	RedisCallback cb;
	cb.fn = std::move(fn);
	cb.privdata = std::move(privdata);

	RedisAsyncCallbackPtr asyncCallback(new RedisAsyncCallback());
	asyncCallback->data = data;
	asyncCallback->len = writableBytes;
	asyncCallback->cb = std::move(cb);

	conn->getLoop()->runInLoop(std::bind(&RedisAsyncContext::proxyAsyncCommand, shared_from_this(), asyncCallback));
	return REDIS_OK;
}

int32_t RedisAsyncContext::threadProxyRedisvAsyncCommand(const RedisCallbackFn &fn,
	const char *data, int32_t len, const std::any &privdata) {
	TcpConnectionPtr conn = weakRedisConn.lock();
	assert(conn != nullptr);
	conn->getLoop()->assertInLoopThread();
	if (redisContext->flags == REDIS_DISCONNECTING) {
		return REDIS_ERR;
	}

	char *buffer = (char *)zmalloc(len);
	memcpy(buffer, data, len);

	RedisCallback cb;
	cb.fn = std::move(fn);
	cb.privdata = std::move(privdata);

	RedisAsyncCallbackPtr asyncCallback(new RedisAsyncCallback());
	asyncCallback->data = buffer;
	asyncCallback->len = len;
	asyncCallback->cb = std::move(cb);

	conn->getLoop()->runInLoop(std::bind(&RedisAsyncContext::__redisAsyncCommand, shared_from_this(), asyncCallback));
	return REDIS_OK;
}

int32_t RedisAsyncContext::redisvAsyncCommand(const RedisCallbackFn &fn, const std::any &privdata, const char *format,
	va_list ap) {
	if (redisContext->flags == REDIS_DISCONNECTING) {
		return REDIS_ERR;
	}

	char *cmd;
	int32_t len;
	len = redisvFormatCommand(&cmd, format, ap);
	if (len < 0) {
		return REDIS_ERR;
	}

	RedisCallback cb;
	cb.fn = std::move(fn);
	cb.privdata = std::move(privdata);

	RedisAsyncCallbackPtr asyncCallback(new RedisAsyncCallback());
	asyncCallback->data = cmd;
	asyncCallback->len = len;
	asyncCallback->cb = std::move(cb);

	TcpConnectionPtr conn = weakRedisConn.lock();
	assert(conn != nullptr);
	conn->getLoop()->runInLoop(std::bind(&RedisAsyncContext::__redisAsyncCommand, shared_from_this(), asyncCallback));
	return REDIS_OK;
}

int32_t RedisAsyncContext::redisAsyncCommand(const RedisCallbackFn &fn, const std::any &privdata, const char *format, ...) {
	va_list ap;
	va_start(ap, format);
	int32_t status = redisvAsyncCommand(fn, privdata, format, ap);
	va_end(ap);
	return status;
}

Hiredis::Hiredis(EventLoop *loop, int16_t sessionCount,
	const char *ip, int16_t port, bool clusterMode)
	: loop(loop),
	pool(new ThreadPool(loop)),
	sessionCount(sessionCount),
	ip(ip),
	port(port),
	pos(0),
	clusterMode(clusterMode) {
	
}

Hiredis::~Hiredis() {

}

void Hiredis::clusterMoveConnCallback(const TcpConnectionPtr &conn) {
	LOG_DEBUG << "";
	if (conn->connected()) {
		const RedisAsyncCallbackPtr &asyncCallback = std::any_cast<RedisAsyncCallbackPtr>(conn->getContext());
		RedisAsyncContextPtr ac(new RedisAsyncContext(conn->intputBuffer(), conn));
		conn->setContext(ac);
		conn->getLoop()->runInLoop(std::bind(&RedisAsyncContext::__redisAsyncCommand, ac, asyncCallback));
		
		if (connectionCallback) {
			connectionCallback(conn);
		}
	}
	else {
		if (disConnectionCallback) {
			disConnectionCallback(conn);
		}
	}
}

void Hiredis::clusterAskConnCallback(const TcpConnectionPtr &conn) {
	LOG_DEBUG << "";
	if (conn->connected()) {
		const RedisAsyncCallbackPtr &asyncCallback = std::any_cast<RedisAsyncCallbackPtr>(conn->getContext());
		RedisAsyncContextPtr ac(new RedisAsyncContext(conn->intputBuffer(), conn));
		conn->setContext(ac);
			
		{
			sds ask = sdsnewlen("*1\r\n$6\r\nASKING\r\n", 16);
			RedisAsyncCallbackPtr asyncCallback(new RedisAsyncCallback);
			asyncCallback->data = ask;
			asyncCallback->len = sdslen(ask);
			asyncCallback->type = true;
			ac->repliesCb.push_back(asyncCallback);
			conn->outputBuffer()->append(ask, sdslen(ask));
		}
	
		conn->getLoop()->runInLoop(std::bind(&RedisAsyncContext::__redisAsyncCommand, ac, asyncCallback));
		
		if (connectionCallback) {
			connectionCallback(conn);
		}
	}
	else {
		if (disConnectionCallback) {
			disConnectionCallback(conn);
		}
	}
}

void Hiredis::redisConnCallback(const TcpConnectionPtr &conn) {
	if (conn->connected()) {
		RedisAsyncContextPtr ac(new RedisAsyncContext(conn->intputBuffer(), conn));
		ac->redisContext->flags = REDIS_CONNECTED;
		conn->setContext(ac);

		if (connectionCallback) {
			connectionCallback(conn);
		}
	}
	else {
		if (disConnectionCallback) {
			disConnectionCallback(conn);
		}
	}
}

std::string Hiredis::setTcpClientInfo(const std::string &errstr, const std::string &ip, int16_t port) {
	std::string result;
	result += "-";
	result += errstr;
	result += " ";
	result += ip;
	std::string p = std::to_string(port);
	result += " :";
	result += p;
	return result;
}

std::string Hiredis::setTcpClientInfo(const std::string &ip, int16_t port) {
	std::string result;
	result += "-Error Could not connect to Redis server ";
	result += ip;
	std::string p = std::to_string(port);
	result += " :";
	result += p;
	result += " Connection refused \r\n";
	return result;
}

RedisAsyncContextPtr Hiredis::getClusterRedisAsyncContext(const std::thread::id &threadId) {
	std::unique_lock <std::mutex> lk(mutex);
	assert(!redisClients.empty());
	for (auto &it : redisClients) {
		auto conn = it->getConnection();
		if (conn == nullptr) {
			continue;
		}
		else {
			conn->getLoop()->assertInLoopThread();
			assert(conn->getContext().has_value());
			const RedisAsyncContextPtr &ac = std::any_cast<RedisAsyncContextPtr>(conn->getContext());
			return ac;
		}
	}
	return nullptr;
}

std::string Hiredis::getTcpClientInfo(const std::thread::id &threadId, int32_t sockfd) {
	std::unique_lock <std::mutex> lk(mutex);
	if (redisClients.empty()) {
		return "-Error Could not connect to Redis server empty Connection refused \r\n";
	}
	else {
		std::string result;
		auto it = redisClientMaps.find(threadId);
		assert(it != redisClientMaps.end());
		auto tcpclient = it->second[(it->second.size() % sockfd) - 1];

		result += "-Error Could not connect to Redis server ";
		result += tcpclient->getIp();
		std::string port = std::to_string(tcpclient->getPort());
		result += ":";
		result += port;
		result += " Connection refused \r\n";
		return result;
	}
}

uint32_t Hiredis::keyHashSlot(char *key, int32_t keylen) {
	int32_t s, e; /* start-end indexes of { and } */

	for (s = 0; s < keylen; s++)
		if (key[s] == '{') break;

	/* No '{' ? Hash the whole key. This is the base case. */
	if (s == keylen) return crc16(key, keylen) & 0x3FFF;

	/* '{' found? Check if we have the corresponding '}'. */
	for (e = s + 1; e < keylen; e++)
		if (key[e] == '}') break;

	/* No '}' or nothing betweeen {} ? Hash the whole key. */
	if (e == keylen || e == s + 1) return crc16(key, keylen) & 0x3FFF;

	/* If we are here there is both a { and a } on its right. Hash
	* what is in the middle between { and }. */
	return crc16(key + s + 1, e - s - 1) & 0x3FFF;
}

int32_t RedisAsyncContext::redisAsyncCommandArgv(const RedisCallbackFn &fn, const std::any &privdata, int32_t argc,
	const char **argv, const int32_t *argvlen) {
	sds cmd;
	int32_t len;
	int32_t status;
	len = redisFormatSdsCommandArgv(&cmd, argc, argv, argvlen);
	RedisCallback cb;
	cb.fn = std::move(fn);
	cb.privdata = std::move(privdata);

	RedisAsyncCallbackPtr asyncCallback(new RedisAsyncCallback());
	asyncCallback->data = cmd;
	asyncCallback->len = len;
	asyncCallback->cb = std::move(cb);
	asyncCallback->type = true;

	TcpConnectionPtr conn = weakRedisConn.lock();
	assert(conn != nullptr);
	conn->getLoop()->runInLoop(std::bind(&RedisAsyncContext::__redisAsyncCommand, shared_from_this(), asyncCallback));
	return REDIS_OK;
}

RedisAsyncContextPtr Hiredis::getRedisAsyncContext(const RedisObjectPtr &command, const std::thread::id &threadId, int32_t sockfd) {
	std::unique_lock <std::mutex> lk(mutex);
	if (redisClients.empty()) {
		return nullptr;
	}

	tmpClients.clear();
	int32_t hashslot = keyHashSlot(command->ptr, sdslen(command->ptr));
	auto migrateIter = clusterMigrate.find(hashslot);
	if (migrateIter != clusterMigrate.end()) {
		for (auto &it : clusterNodes) {
			if (it->id == migrateIter->second) {
				auto iter = redisClientMaps.find(threadId);
				assert(iter != redisClientMaps.end());

				for (auto &iterr : iter->second) {
					if (iterr->getPort() == it->port) {
						tmpClients.push_back(iterr);
					}
				}

				if (tmpClients.empty()) {
					return nullptr;
				}

				int32_t index = ((tmpClients.size() - 1) % sockfd);
				const TcpConnectionPtr &conn = tmpClients[index]->getConnection();
				if (conn == nullptr) {
					return nullptr;
				}
				else {
					assert(conn->getContext().has_value());
					const RedisAsyncContextPtr &ac = std::any_cast<RedisAsyncContextPtr>(conn->getContext());
					return ac;
				}
			}
		}
	}
	
	for (auto &it : clusterNodes) {
		for (auto iter : it->slots) {	
			if (hashslot >= iter.first && hashslot <= iter.second && it->master == "master" && it->status == 1) {
				auto iter = redisClientMaps.find(threadId);
				assert(iter != redisClientMaps.end());

				for (auto &iterr : iter->second) {
					if (iterr->getPort() == it->port) {
						tmpClients.push_back(iterr);
					}
				}

				if (tmpClients.empty()) {
					return nullptr;
				}

				int32_t index = ((tmpClients.size() - 1) % sockfd);
				const TcpConnectionPtr &conn = tmpClients[index]->getConnection();
				if (conn == nullptr) {
					return nullptr;
				}
				else {
					assert(conn->getContext().has_value());
					const RedisAsyncContextPtr &ac = std::any_cast<RedisAsyncContextPtr>(conn->getContext());
					return ac;
				}
			}
		}
	}
	return nullptr;
}

RedisAsyncContextPtr Hiredis::getRedisAsyncContext(const std::thread::id &threadId, int32_t sockfd) {
	std::unique_lock <std::mutex> lk(mutex);
	if (redisClients.empty()) {
		return nullptr;
	}

	auto it = redisClientMaps.find(threadId);
	assert(it != redisClientMaps.end());

	int32_t index = (it->second.size() - 1) % sockfd;

	const TcpConnectionPtr &conn = it->second[index]->getConnection();
	if (conn == nullptr) {
		return nullptr;
	}
	else {
		assert(conn->getContext().has_value());
		const RedisAsyncContextPtr &ac = std::any_cast<RedisAsyncContextPtr>(conn->getContext());
		return ac;
	}
}

RedisContextPtr Hiredis::getRedisContext() {
	std::unique_lock <std::mutex> lk(mutex);
	if (redisContexts.empty()) {
		return nullptr;
	}
	return redisContexts[0];
}

RedisContextPtr Hiredis::getRedisContext(const int32_t sockfd) {
	std::unique_lock <std::mutex> lk(mutex);
	if (redisContexts.empty()) {
		return nullptr;
	}
	return redisContexts[(redisContexts.size() - 1) % sockfd];
}

std::vector <RedisContextPtr> Hiredis::getRedisContext(const std::thread::id &threadId) {
	std::unique_lock <std::mutex> lk(mutex);
	tmpAsync.clear();

	if (redisContexts.empty()) {
		return tmpAsync;
	}

	for (auto &it : clusterNodes) {
		if (it->master == "master" && it->status == 1 && (!it->slots.empty())) {
			for (auto &iter : redisContexts) {
				if (iter->ip == it->ip && it->port == iter->port) {
					tmpAsync.push_back(iter);
					break;
				}
			}
		}
	}
	return tmpAsync;
}

RedisAsyncContextPtr Hiredis::getRedisAsyncContext(int32_t sockfd) {
	std::unique_lock <std::mutex> lk(mutex);
	if (redisClients.empty()) {
		return nullptr;
	}

	const TcpConnectionPtr &conn = redisClients[(redisClients.size() - 1) % sockfd]->getConnection();
	if (conn == nullptr) {
		return nullptr;
	}
	else {
		assert(conn->getContext().has_value());
		const RedisAsyncContextPtr &ac = std::any_cast<RedisAsyncContextPtr>(conn->getContext());
		return ac;
	}
}

RedisAsyncContextPtr Hiredis::getRedisAsyncContext() {
	std::unique_lock <std::mutex> lk(mutex);
	if (redisClients.empty()) {
		return nullptr;
	}

	const TcpConnectionPtr &conn = redisClients[0]->getConnection();
	if (conn == nullptr) {
		return nullptr;
	}
	else {
		assert(conn->getContext().has_value());
		const RedisAsyncContextPtr &ac = std::any_cast<RedisAsyncContextPtr>(conn->getContext());
		return ac;
	}
}

void Hiredis::diconnectTcpClient() {
	std::unique_lock <std::mutex> lk(mutex);
	for (auto &it : redisClients) {
		it->disConnect();
	}
}

void Hiredis::clearTcpClient() {
	std::unique_lock <std::mutex> lk(mutex);
	redisClients.clear();
	redisContextMaps.clear();
}

void Hiredis::pushTcpClient(const TcpClientPtr &client) {
	redisClients.push_back(client);
}

void Hiredis::pushRedisContext(const RedisContextPtr &context) {
	redisContexts.push_back(context);
}

void Hiredis::redisGetSubscribeCallback(const RedisAsyncContextPtr &ac, const RedisReplyPtr &reply,
	RedisAsyncCallbackPtr &callback) {
	int32_t pvariant;
	char *stype;
	sds sname;

	/* Custom reply functions are not supported for pub/sub. This will fail
	* very hard when they are used... */
	if (reply->type == REDIS_REPLY_ARRAY) {
		assert(reply->element.size() >= 2);
		assert(reply->element[0]->type == REDIS_REPLY_STRING);
		stype = reply->element[0]->str;
		pvariant = (tolower(stype[0]) == 'p') ? 1 : 0;

		/* Locate the right callback */
		assert(reply->element[1]->type == REDIS_REPLY_STRING);
		sname = sdsnewlen(reply->element[1]->str, sdslen(reply->element[1]->str));
		RedisObjectPtr redisObj = createObject(REDIS_STRING, sname);
		if (pvariant) {
			auto iter = ac->subCb.patternCb.find(redisObj);
			if (iter != ac->subCb.patternCb.end()) {
				callback = iter->second;
				/* If this is an unsubscribe message, remove it. */
				if (MEMCMP(stype + pvariant, "unsubscribe", 11) == 0) {
					ac->subCb.patternCb.erase(iter);

					/* If this was the last unsubscribe message, revert to
						* non-subscribe mode. */
					assert(reply->element[2]->type == REDIS_REPLY_INTEGER);
					if (reply->element[2]->integer == 0)
						ac->redisContext->flags = REDIS_CONNECTED;
				}
			}
		}
		else {
			auto iter = ac->subCb.channelCb.find(redisObj);
			if (iter != ac->subCb.channelCb.end()) {
				callback = iter->second;
				/* If this is an unsubscribe message, remove it. */
				if (MEMCMP(stype + pvariant, "unsubscribe", 11) == 0) {
					ac->subCb.channelCb.erase(iter);

					/* If this was the last unsubscribe message, revert to
						* non-subscribe mode. */
					assert(reply->element[2]->type == REDIS_REPLY_INTEGER);
					if (reply->element[2]->integer == 0)
						ac->redisContext->flags = REDIS_CONNECTED;
				}
			}
		}
	}
	else {
		/* Shift callback for invalid commands. */
		assert(!ac->subCb.invalidCb.empty());
		callback = ac->subCb.invalidCb.front();
		ac->subCb.invalidCb.pop_front();
	}
}

void Hiredis::redisAsyncDisconnect(const RedisAsyncContextPtr &ac, const RedisReplyPtr &reply) {
	ac->err = ac->redisContext->err;
	ac->errstr = ac->redisContext->errstr;

	if (ac->err == 0) {
		/* For clean disconnects, there should be no pending callbacks. */
		if (!ac->repliesCb.empty()) {
			ac->repliesCb.pop_front();
		}
	}
	else {
		/* Disconnection is caused by an error, make sure that pending
		* callbacks cannot call new commands. */
		ac->redisContext->flags = REDIS_DISCONNECTING;
	}

	{
		for (auto &iter : ac->repliesCb) {
			if (iter->cb.fn) {
				iter->cb.fn(ac, reply, iter->cb.privdata);
			}
		}
		ac->repliesCb.clear();
	}

	{
		for (auto &iter : ac->subCb.invalidCb) {
			if (iter->cb.fn) {
				iter->cb.fn(ac, reply, iter->cb.privdata);
			}
		}
		ac->subCb.invalidCb.clear();
	}

	{
		for (auto &iter : ac->subCb.patternCb) {
			if (iter.second->cb.fn) {
				iter.second->cb.fn(ac, reply, iter.second->cb.privdata);
			}
		}
		ac->subCb.patternCb.clear();
	}

	{
		for (auto &iter : ac->subCb.channelCb) {
			if (iter.second->cb.fn) {
				iter.second->cb.fn(ac, reply, iter.second->cb.privdata);
			}
		}
		ac->subCb.channelCb.clear();
	}

	TcpConnectionPtr conn = ac->getTcpConnection().lock();
	assert(conn != nullptr);
	conn->forceCloseInLoop();
}

void Hiredis::connect(EventLoop *loop, const TcpClientPtr &client, int32_t count) {
	if (count > 0) {
		client->connect(true);
	}
	else {
		client->connect();
	}

	auto c = redisConnect(client->getIp(), client->getPort());
	if (c == nullptr || c->err) {
		if (c) {
			LOG_WARN << "Hiredis connection error: " << c->errstr;
		}
	}
	else {
		struct timeval tv = { kTimeOut, 0 };
		Socket::setTimeOut(c->fd, tv);
	}

	std::unique_lock <std::mutex> lk(mutex);
	{
		auto it = redisClientMaps.find(loop->getThreadId());
		if (it == redisClientMaps.end()) {
			std::vector <TcpClientPtr> clients;
			clients.push_back(client);
			redisClientMaps[loop->getThreadId()] = std::move(clients);
		}
		else {
			it->second.push_back(client);
		}
		pushTcpClient(client);
	}

	{
		auto it = redisContextMaps.find(loop->getThreadId());
		if (it == redisContextMaps.end()) {
			std::vector <RedisContextPtr> clients;
			clients.push_back(c);
			redisContextMaps[loop->getThreadId()] = std::move(clients);
		}
		else {
			it->second.push_back(c);
		}
		pushRedisContext(c);
	}
}

void Hiredis::poolStart() {
	pool->start();
}

void Hiredis::start() {
	auto vectors = pool->getAllLoops();
	for (int32_t i = 0; i < vectors.size(); i++) {
		start(vectors[i], 0, ip, port);
	}
}

void Hiredis::startTimer() {
	loop->runAfter(kTimer, true, std::bind(&Hiredis::redisContextTimer, shared_from_this()));
}
void Hiredis::start(EventLoop *loop, int32_t count, const char *ip, int16_t port) {
	for (int32_t j = 0; j < sessionCount; j++) {
		TcpClientPtr client(new TcpClient(loop, ip, port, nullptr));
		client->enableRetry();
		client->setConnectionCallback(std::bind(&Hiredis::redisConnCallback, shared_from_this(), std::placeholders::_1));
		client->setMessageCallback(
			std::bind(&Hiredis::redisReadCallback, shared_from_this(), std::placeholders::_1, std::placeholders::_2));
		connect(loop, client, count);
	}
}

void Hiredis::redisReadCallback(const TcpConnectionPtr &conn, Buffer *buffer) {
	assert(conn->getContext().has_value());
	const RedisAsyncContextPtr &ac = std::any_cast<RedisAsyncContextPtr>(conn->getContext());
	RedisReplyPtr reply;
	int32_t status;

	while ((status = ac->redisContext->redisGetReply(reply)) == REDIS_OK) {
		if (reply == nullptr) {
			/* When the connection is being disconnected and there are
				 * no more replies, this is the cue to really disconnect. */
			if (ac->redisContext->flags == REDIS_DISCONNECTING && buffer->readableBytes() == 0
				&& ac->repliesCb.empty()) {
				LOG_DEBUG << "";
				redisAsyncDisconnect(ac, nullptr);
				return;
			}

			/* When the connection is not being disconnected, simply stop
				 * trying to get replies and wait for the next loop tick. */
			break;
		}

		/* Even if the context is subscribed, pending regular callbacks will
		   * get a reply before pub/sub messages arrive. */
		RedisAsyncCallbackPtr repliesCb = nullptr;
		if (ac->repliesCb.empty()) {
			LOG_DEBUG << "";
			/*
			* A spontaneous reply in a not-subscribed context can be the error
			* reply that is sent when a new connection exceeds the maximum
			* number of allowed connections on the server side.
			*
			* This is seen as an error instead of a regular reply because the
			* server closes the connection after sending it.
			*
			* To prevent the error from being overwritten by an EOF error the
			* connection is closed here. See issue #43.
			*
			* Another possibility is that the server is loading its dataset.
			* In this case we also want to close the connection, and have the
			* user wait until the server is ready to take our request.
			*/

			if (reply->type == REDIS_REPLY_ERROR) {
				redisAsyncDisconnect(ac, reply);
				return;
			}

			assert((ac->redisContext->flags == REDIS_SUBSCRIBED || ac->redisContext->flags == REDIS_MONITORING));

			/* No more regular callbacks and no errors, the context *must* be subscribed or monitoring. */
			if (ac->redisContext->flags == REDIS_SUBSCRIBED) {
				redisGetSubscribeCallback(ac, reply, repliesCb);
			}
		}
		else {
			repliesCb = ac->repliesCb.front();
			ac->repliesCb.pop_front();
		}

		if (reply->type == REDIS_REPLY_ERROR &&
			(!strncmp(reply->str, "MOVED", 5) || (!strncmp(reply->str, "ASK", 3)))) {
			char *p = reply->str, *s;
			int32_t slot;
			s = strchr(p, ' ');
			p = strchr(s + 1, ' ');
			*p = '\0';
			slot = atoi(s + 1);
			s = strrchr(p + 1, ':');
			*s = '\0';

			const char *ip = p + 1;
			int16_t port = atoi(s + 1);
			LOG_WARN << "-> Redirected to slot " << slot << " located at " << ip << " " << port;
			auto redirectConn = redirectySlot(conn->getSockfd(), conn->getLoop(), ip, port);
			if (redirectConn == nullptr) {
				TcpClientPtr client(new TcpClient(conn->getLoop(), ip, port, repliesCb));
				client->setMessageCallback(
					std::bind(&Hiredis::redisReadCallback, shared_from_this(), std::placeholders::_1, std::placeholders::_2));
				if (!strncmp(reply->str, "MOVED", 5)) {
					client->setConnectionCallback(
						std::bind(&Hiredis::clusterMoveConnCallback, shared_from_this(), std::placeholders::_1));
				}
				else if (!strncmp(reply->str, "ASK", 3)) {
					client->setConnectionCallback(
						std::bind(&Hiredis::clusterAskConnCallback, shared_from_this(), std::placeholders::_1));
				}

				client->enableRetry();
				connect(conn->getLoop(), client, 1);
				client->setConnectionCallback(std::bind(&Hiredis::redisConnCallback, shared_from_this(), std::placeholders::_1));
			}
			else {
				assert(redirectConn->getContext().has_value());
				RedisAsyncContextPtr acContext = std::any_cast<RedisAsyncContextPtr>(redirectConn->getContext());
				
				if (!strncmp(reply->str, "ASK", 3)) {
					sds ask = sdsnewlen("*1\r\n$6\r\nASKING\r\n", 16);
					RedisAsyncCallbackPtr asyncCallback(new RedisAsyncCallback);
					asyncCallback->data = ask;
					asyncCallback->len = sdslen(ask);
					asyncCallback->type = true;
					redirectConn->getLoop()->runInLoop(
						std::bind(&RedisAsyncContext::__redisAsyncCommand, acContext, asyncCallback));
				} 
				redirectConn->getLoop()->runInLoop(
					std::bind(&RedisAsyncContext::__redisAsyncCommand, acContext, repliesCb));
			}
		}
		else {
			if (ac->redisContext->flags == REDIS_MONITORING) {

				/* If monitor mode, repush callback */
				ac->repliesCb.push_back(repliesCb);
				reply->type = REDIS_REPLY_MONITOR;
			}

			if (repliesCb->cb.fn) {
				repliesCb->cb.fn(ac, reply, repliesCb->cb.privdata);
			}
		}
	}

	/* Disconnect when there was an error reading the reply */
	if (status != REDIS_OK) {
		LOG_DEBUG << "";
		redisAsyncDisconnect(ac, nullptr);
	}
}

TcpConnectionPtr Hiredis::redirectySlot(int32_t sockfd, EventLoop *loop, const char *ip, int16_t port) {
	std::unique_lock <std::mutex> lk(mutex);
	assert(!redisClients.empty());
	auto it = redisClientMaps.find(loop->getThreadId());
	assert(it != redisClientMaps.end());

	TcpConnectionPtr conn = nullptr;
	for (auto &iter : it->second) {
		if (iter->getPort() == port) {
			moveAskClients.push_back(iter->getConnection());
		}
	}

	if (moveAskClients.empty()) {
		LOG_WARN << "-> Add new cluster client ip port " << ip << " " << port;
	}
	else {
		int32_t index = (moveAskClients.size() - 1) % sockfd;
		conn = moveAskClients[index];
		moveAskClients.clear();
	}
	return conn;
}

void Hiredis::redisContextTimer() {
	loop->assertInLoopThread();
	auto it = redisContextMaps.find(loop->getThreadId());
	if (it == redisContextMaps.end()) {
		return;
	}

	clusterMigrate.clear();
	bool node = false;
	for (auto &iter : it->second) {
		RedisReplyPtr reply = iter->redisCommand("PING");
		if (reply == nullptr || !(strcmp(reply->str, "PONG") == 0)
			|| reply->type != REDIS_REPLY_STATUS) {
			if (reply != nullptr) {
				LOG_WARN << "Redis reply error :" << reply->str;
			}
			else {
				LOG_WARN << "Redis reply error :" << iter->errstr;
			}
			
			if (iter->redisReconnect() == REDIS_ERR) {
				LOG_WARN << "RedisContext reconnect err " << iter->errstr;
				continue ;
			}
		}
		
		if (!clusterMode) {
			return ;
		}
		
		if (!node) {
			node = true;
			reply = iter->redisCommand("cluster nodes");
			if (reply != nullptr && reply->type == REDIS_REPLY_STRING) {
				clusterNodes.clear();
				//LOG_INFO << reply->str;
				int32_t index = 0;
				while (index < sdslen(reply->str)) {
					std::shared_ptr <ClusterNode> node(new ClusterNode());
					const char *id = strchr(reply->str + index, ' ');
					assert(id != nullptr);

					node->id = std::string(reply->str + index, id - (reply->str + index));
					const char *ip = strchr(id + 1, ':');
					assert(ip != nullptr);

					node->ip = std::string(id + 1, ip - id - 1);
					const char *port = strchr(ip + 1, '@');
					assert(port != nullptr);

					int16_t p = atoi(ip + 1);
					node->port = p;
					const char *p1 = strchr(port + 1, ' ');
					assert(p1 != nullptr);

					const char *myself = strchr(p1 + 1, ' ');
					std::string m = std::string(p1 + 1, myself - p1 - 1);
					if (m == "myself,master" || m == "master") {
						node->master = "master";
						const char *end = strchr(myself + 1, '\n');
						assert(end != nullptr);
						index = end + 1 - reply->str;
						
						const char *p1 = strchr(myself + 1, ' ');
						assert(p1 != nullptr);
						const char *p2 = strchr(p1 + 1, ' ');
						assert(p2 != nullptr);
						const char *p3 = strchr(p2 + 1, ' ');
						assert(p3 != nullptr);
						const char *p4 = strchr(p3 + 1, ' ');
						assert(p4 != nullptr);
						const char *p5 = strchr(p4 + 1, ' ');
						if (p5 == nullptr || ((p5 - reply->str) > index)) {
							if (MEMCMP(p4 + 1, "connected", end - (p4 + 1)) == 0) {
								node->status = 1;
							} else {
								node->status = 0;
							}
							
							clusterNodes.push_back(node);
							continue;
						}
						
						assert(p5 != nullptr);
						if (MEMCMP(p4 + 1, "connected", p5 - p4 - 1) == 0) {
							node->status = 1;
						} else {
							node->status = 0;
						}
						
						const char *slot = p5;
						while (1) {
							int16_t startSlot = atoi(slot + 1);
							const char *p6 = std::find(slot, end, ' ');
							if (p6 == end) {
								node->migrate = false;	
								const char *p7 = std::find(p6 + 1, end, '-');
								if (p7 != nullptr) {
									int16_t endSlot = atoi(p7 + 1);
									node->slots[startSlot] = endSlot;
								} else {
									node->slots[startSlot] = startSlot;
								}
								break;
							}
							
							if (*(p6 + 1) == '[') {
								node->migrate = true;
								const char *p7 = strchr(slot, '[');
								assert(p7 != nullptr);
								int16_t migrateSlot = atoi(p7 + 1);
								const char *p8= strchr(p7 + 1, '-');
								assert(p8 != nullptr);
								const char *p9 = strchr(p8 + 1, '-');
								assert(p9 != nullptr);
								const char *p10 = strchr(p9+ 1, ']');
								assert(p10 != nullptr);
								std::string migrateId = std::string(p9 + 1, p10 - p9 - 1);
								clusterMigrate[migrateSlot] = migrateId;
								break;
							}
							
							const char *p7 = std::find(p6 + 1, end, '-');
							if (p7 == end) {
								slot = p6 + 1 + std::to_string(startSlot).size();
								continue;
							}
							
							int16_t endSlot = atoi(p7 + 1);
							const char *p8 = strchr(p7 + 1, '\n');			
							node->slots[startSlot] = endSlot;
							size_t size = std::to_string(endSlot).size();
							if (std::to_string(endSlot).size() >= end - p7 - 1) {
								break;
							}
							slot = p7 + 1 + size;
						}
						clusterNodes.push_back(node);
					}
					else {
						node->slave = m;
						const char *p1 = strchr(myself + 1, ' ');
						assert(p1 != nullptr);
						std::string master = std::string(myself + 1, p1 - myself - 1);
						const char *p2 = strchr(p1 + 1, ' ');
						assert(p2 != nullptr);
						const char *p3 = strchr(p2 + 1, ' ');
						assert(p3 != nullptr);
						const char *p4 = strchr(p3 + 1, ' ');
						assert(p4 != nullptr);
						const char *p5 = strchr(p4 + 1, '\n');
						assert(p5 != nullptr);
						index = p5 + 1 - reply->str;
						clusterNodes.push_back(node);
					}
				}
			}
			else {
				LOG_WARN << "Redis cluster nodes error ";
				return;
			}
		}
				
		clusterConnectInfos.clear();
		clusterDelNodes.clear();

		for (auto &it : clusterNodes) {
			if (it->master == "master" && it->status == 1 && (!it->slots.empty())) {
				clusterDelNodes[it->port] = it->ip;
				bool mark = false;
				for (auto &iter : redisClients) {
					if (iter->getPort() == it->port) {
						mark = true;
						break;
					}
				}

				if (!mark) {
					clusterConnectInfos[it->port] = it->ip;
				}
			}
		}
	}
	
	if (!clusterMode) {
		return ;
	}
	
	/*LOG_INFO << "---------------";
	for (auto &it : clusterNodes) {
		LOG_INFO << "id:" << it->id << " ip:" << it->ip << " port:" << it->port << " master: " <<it->master << " slave:" << it->slave;
		for (auto &iter : it->slots) {
			LOG_INFO << iter.first << " " << iter.second;
		}
	}
	*/
			
	for (auto &it : clusterConnectInfos) {
		//LOG_INFO << "ip " << it.second << " " << it.first;
		start(loop, 1, it.second.c_str(), it.first);
	}
	
	if (clusterNodes.empty()) {
		return ;
	}

	for (auto it = redisClients.begin(); it != redisClients.end();) {
		bool mark = false;
		for (auto &iter : clusterDelNodes) {
			if (iter.first == (*it)->getPort()) {
				mark = true;
				break;
			}
		}

		if (!mark) {
			(*it)->disConnect();
		    it = redisClients.erase(it);
		} else {
			++it;
		}
	}

	for (auto it = redisContexts.begin(); it != redisContexts.end();) {
		bool mark = false;
		for (auto &iter : clusterDelNodes) {
			if (iter.first == (*it)->getPort()) {
				mark = true;
				break;
			}
		}

		if (!mark) {
			it = redisContexts.erase(it);
		}
		else {
			++it;
		}
	}

	auto contexts = redisContextMaps.find(loop->getThreadId());
	assert(contexts != redisContextMaps.end());

	for (auto it = contexts->second.begin(); it != contexts->second.end();) {
		bool mark = false;
		for (auto &iter : clusterDelNodes) {
			if (iter.first == (*it)->getPort()) {
				mark = true;
				break;
			}
		}

		if (!mark) {
			it = contexts->second.erase(it);
		}
		else {
			++it;
		}
	}

	auto clients = redisClientMaps.find(loop->getThreadId());
	assert(clients != redisClientMaps.end());

	for (auto it = clients->second.begin(); it != clients->second.end();) {
		bool mark = false;
		for (auto &iter : clusterDelNodes) {
			if (iter.first == (*it)->getPort()) {
				mark = true;
				break;
			}
		}

		if (!mark) {
			(*it)->disConnect();
			it = clients->second.erase(it);
		}
		else {
			++it;
		}
	}
}

RedisContextPtr redisConnectUnix(const char *path) {
	RedisContextPtr c(new RedisContext());
	c->redisContextConnectUnix(path, nullptr);
	return c;
}

/* Connect to a Redis instance. On error the field error in the returned
 * context will be set to the return value of the error function.
 * When no set of reply functions is given, the default set will be used. */
RedisContextPtr redisConnect(const char *ip, int16_t port) {
	RedisContextPtr c(new RedisContext());
	c->redisContextConnectTcp(ip, port, nullptr);
	return c;
}

RedisContextPtr redisConnectWithTimeout(const char *ip, int16_t port, const struct timeval tv) {
	RedisContextPtr c(new RedisContext());
	c->redisContextConnectTcp(ip, port, &tv);
	return c;
}
