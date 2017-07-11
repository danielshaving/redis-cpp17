#include "xRdb.h"

xRio::xRio()
{

}

xRio::~xRio()
{

}


off_t rioTell(xRio *r)
{
	return r->tellFuc(r);
}

off_t rioFlush(xRio *r)
{
	return r->flushFuc(r);
}

size_t rioWrite(xRio *r,const void *buf,size_t len)
{
	while(len)
	{
		size_t bytesToWrite = (r->maxProcessingChunk && r->maxProcessingChunk < len)?r->maxProcessingChunk:len;
		if(r->updateFuc)
		{
			r->updateFuc(r,buf,bytesToWrite);
		}

		if(r->writeFuc(r,buf,bytesToWrite) == 0)
		{
			return 0;
		}

		buf = (char*)buf + bytesToWrite;
		len -= bytesToWrite;
		r->processedBytes += bytesToWrite;
	}

	return 1;
}

size_t rioRead(xRio *r,void  *buf,size_t len)
{
	while(len)
	{
		size_t bytesToRead = (r->maxProcessingChunk && r->maxProcessingChunk <len)? r->maxProcessingChunk:len;
		if(r->readFuc(r,buf,bytesToRead) == 0)
		{
			return 0;
		}

		if(r->updateFuc)
		{
			r->updateFuc(r,buf,bytesToRead);
		}

		buf = (char*)buf + bytesToRead;
		len -= bytesToRead;
		r->processedBytes += bytesToRead;
	}

	return 1;
}

size_t rioFileRead(xRio*r, void *buf, size_t len)
{
	return fread(buf,len,1,r->io.file.fp);
}

size_t rioFileWrite(xRio *r, const void *buf, size_t len)
{
	 size_t retval;
	 retval = fwrite(buf,len,1,r->io.file.fp);
	 r->io.file.buffered += len;

	 if(r->io.file.autosync && r->io.file.buffered >= r->io.file.autosync)
	 {
	 	fflush(r->io.file.fp);
	 }

	 return retval;
}

off_t rioFileTell(xRio *r)
{
	return ftello(r->io.file.fp);
}

int rioFileFlush(xRio *r)
{
	return (fflush(r->io.file.fp) == 0) ? 1:0;
}


void rioGenericUpdateChecksum(xRio *r, const void *buf, size_t len)
{
	r->cksum = crc64(r->cksum,(const unsigned char*)buf,len);
}

void rioInitWithFile(xRio *r, FILE *fp)
{
	r->readFuc = rioFileRead;
	r->writeFuc = rioFileWrite;
	r->tellFuc = rioFileTell;
	r->flushFuc =rioFileFlush;
	r->updateFuc = rioGenericUpdateChecksum;
	r->cksum = 0;
	r->processedBytes = 0;
	r->maxProcessingChunk = 65536;
	r->io.file.fp = fp;
	r->io.file.buffered = 0;
	r->io.file.autosync = 0;
}

void rioInitWithBuffer(xRio *r, sds s)
{

}





int rdbEncodeInteger(long long value, unsigned char *enc)
{
	if (value >= -(1<<7) && value <= (1<<7)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
        enc[0] = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc)
{
	long long value;
	char *endptr,buf[32];

	value = strtoll(s,&endptr,10);
	if(endptr[0] != '\0')
	{
		return 0;
	}
	
	ll2string(buf,32,value);
	if (strlen(buf) != len || memcmp(buf,s,len)) 
	{
		return 0;	
	}

    return rdbEncodeInteger(value,enc);
	
}

void startLoading(FILE *fp)
{
	struct stat sb;
	if(fstat(fileno(fp),&sb) == -1)
	{
	
	}
	else
	{
		//TRACE("load dump.rdb %ld\n",sb.st_size);
	}
}

uint32_t digits10(uint64_t v)
{
	if (v < 10) return 1;
    if (v < 100) return 2;
    if (v < 1000) return 3;
    if (v < 1000000000000UL) {
        if (v < 100000000UL) {
            if (v < 1000000) {
                if (v < 10000) return 4;
                return 5 + (v >= 100000);
            }
            return 7 + (v >= 10000000UL);
        }
        if (v < 10000000000UL) {
            return 9 + (v >= 1000000000UL);
        }
        return 11 + (v >= 100000000000UL);
    }
    return 12 + digits10(v / 1000000000000UL);
}

int lll2string(char* dst, size_t dstlen, long long svalue)
{
	static const char digits[201] =
	    "0001020304050607080910111213141516171819"
	    "2021222324252627282930313233343536373839"
	    "4041424344454647484950515253545556575859"
	    "6061626364656667686970717273747576777879"
	    "8081828384858687888990919293949596979899";
	int negative;
	unsigned long long value;

	/* The main loop works with 64bit unsigned integers for simplicity, so
	 * we convert the number here and remember if it is negative. */
	if (svalue < 0) {
	    if (svalue != LLONG_MIN) {
	        value = -svalue;
	    } else {
	        value = ((unsigned long long) LLONG_MAX)+1;
	    }
	    negative = 1;
	} else {
	    value = svalue;
	    negative = 0;
	}

	/* Check length. */
	uint32_t const length = digits10(value)+negative;
	if (length >= dstlen) return 0;

	/* Null term. */
	uint32_t next = length;
	dst[next] = '\0';
	next--;
	while (value >= 100) {
	    int const i = (value % 100) * 2;
	    value /= 100;
	    dst[next] = digits[i + 1];
	    dst[next - 1] = digits[i];
	    next -= 2;
	}

	/* Handle last 1-2 digits. */
	if (value < 10) {
	    dst[next] = '0' + (uint32_t) value;
	} else {
	    int i = (uint32_t) value * 2;
	    dst[next] = digits[i + 1];
	    dst[next - 1] = digits[i];
	}

	/* Add sign. */
	if (negative) dst[0] = '-';
	return length;
}


uint32_t rdbLoadLen(xRio *rdb, int *isencoded)
{
	unsigned char buf[2];
	uint32_t len;
	int type;
	if(isencoded)
	{
		*isencoded = 0;
	}

	if(rioRead(rdb,buf,1) == 0)
	{
		return REDIS_RDB_LENERR;
	}
	type = (buf[0]&0xC0)>>6;

	if(type == REDIS_RDB_ENCVAL)
	{
		if(isencoded)
		{
			*isencoded = 1;
		}
		return buf[0]&0x3F;
	}
	else if(type == REDIS_RDB_6BITLEN)
	{
		return buf[0]&0x3F;
	}
	else if(type == REDIS_RDB_14BITLEN)
	{
		if (rioRead(rdb,buf+1,1) == 0)
		{
			return REDIS_RDB_LENERR;	
		}
        return ((buf[0]&0x3F)<<8)|buf[1];
	}
	else
	{
		if (rioRead(rdb,&len,4) == 0)
		{
			return REDIS_RDB_LENERR;
		}
        return ntohl(len);
	}
	
}

rObj *rdbLoadIntegerObject(xRio *rdb, int enctype, int encode)
{
	unsigned char enc[4];
	long long val;

	if(enctype == REDIS_RDB_ENC_INT8)
	{
		if(rioRead(rdb,enc,1) == 0)
		{
			return nullptr;
		}
		val = (signed char)enc[0];
	}
	else if(enctype == REDIS_RDB_ENC_INT16)
	{
		uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
	}
	else if (enctype == REDIS_RDB_ENC_INT32)
	{
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } 
	else 
	{
        val = 0; /* anti-warning */
        //TRACE("Unknown RDB integer encoding type");
    }

	if (encode)
        return createStringObjectFromLongLong(val);
    else
        return createObject(REDIS_STRING,sdsfromlonglong(val));
	
	
}


rObj *rdbLoadEncodedStringObject(xRio *rdb)
{
	return rdbGenericLoadStringObject(rdb,1);
}


rObj *rdbLoadLzfStringObject(xRio *rdb) {
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    if ((clen = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(rdb,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((c = (unsigned char *)zmalloc(clen)) == NULL) goto err;
    if ((val = sdsnewlen(NULL,len)) == NULL) goto err;
    if (rioRead(rdb,c,clen) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) == 0) goto err;
    zfree(c);
    zfree(val);
    return createStringObject(val,len);
err:
    zfree(c);
    sdsfree(val);
    return NULL;
}


rObj *rdbGenericLoadStringObject(xRio *rdb, int encode)
{
    int isencoded;
    uint32_t len;
    rObj *o;

    len = rdbLoadLen(rdb,&isencoded);
    if (isencoded) {
        switch(len) {
        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            return rdbLoadIntegerObject(rdb,len,encode);
        case REDIS_RDB_ENC_LZF:
           	return rdbLoadLzfStringObject(rdb);
        default:
            //TRACE("Unknown RDB encoding type");
            break;
        }
    }

    if (len == REDIS_RDB_LENERR)
	{
		return nullptr;
    }
    o = createStringObject(NULL,len);
    if (len && rioRead(rdb,(void*)o->ptr,len) == 0) 
	{
        return nullptr;
    }
    
    return o;
}




rObj *rdbLoadStringObject(xRio *rdb)
{
	 return rdbGenericLoadStringObject(rdb,0);
}




int rdbSaveSet(xRio *rdb,xRedis * redis)
{
	for(auto it = redis->setMapShards.begin(); it != redis->setMapShards.end(); it++)
	{
		auto &map = (*it).setMap;
		MutexLock & mu = (*it).mutex;
		MutexLockGuard lock(mu);
		for(auto iter = map.begin(); iter != map.end(); iter++)
		{
			 if (rdbSaveKeyValuePair(rdb,iter->first,iter->second,0) == -1)
			 {
			 	return REDIS_ERR;
			 }
		}
	}
	
	if (rdbSaveType(rdb,REDIS_RDB_SET) == -1) return REDIS_ERR;

	return REDIS_OK;
}
int rdbSaveHset(xRio *rdb,xRedis * redis)
{
	for(auto it = redis->hsetMapShards.begin(); it != redis->hsetMapShards.end(); it++)
	{
		auto &map = (*it).hsetMap;
		MutexLock & mu = (*it).mutex;
		MutexLockGuard lock(mu);
		for(auto iter = map.begin(); iter != map.end(); iter++)
		{
			if (rdbSaveKey(rdb,iter->first,0) == -1)
			{
			 	return REDIS_ERR;
			}

			if((rdbSaveLen(rdb,iter->second.size()) == -1))
			{
				return REDIS_ERR;
			}

			for(auto iterr = iter->second.begin(); iterr != iter->second.end(); iterr++)
			{	
				 if (rdbSaveKeyValuePair(rdb,iterr->first,iterr->second,0) == -1)
				 {
				 	return REDIS_ERR;
				 }
			}
			
			
		}
	}
	
	if (rdbSaveType(rdb,REDIS_RDB_HSET) == -1) return REDIS_ERR;

	return REDIS_OK;
}


int rdbLoadSet(xRio *rdb,xRedis * redis)
{
	int type;
	while(1)
	{
		rObj *key,*val;

		if ((type = rdbLoadType(rdb)) == -1)
		{
			return  REDIS_ERR;
		}

		if (type == REDIS_RDB_SET)
		{
			return REDIS_OK;
		}
		  
		if ((key = rdbLoadStringObject(rdb)) == nullptr)
		{
			return  REDIS_ERR;
		}

		if ((val = rdbLoadObject(type,rdb)) == nullptr)
		{
			return REDIS_ERR;
		}

		key->calHash();
		size_t hash = key->hash;
		MutexLock &mu = redis->setMapShards[hash% redis->kShards].mutex;
		auto & setMap = redis->setMapShards[hash % redis->kShards].setMap;
		{
			MutexLockGuard lock(mu);
			auto it = setMap.find(key);
			if(it == setMap.end())
			{
				setMap.insert(std::make_pair(key,val));
			}
			else
			{
				zfree(it->first);
				zfree(it->second);
				setMap.erase(it);
				setMap.insert(std::make_pair(key,val));
			}
		}
	}
	
	return REDIS_OK;
}

int rdbLoadHset(xRio *rdb,xRedis * redis)
{
	int type,rdbver;
	int len;
	while(1)
	{
		rObj *key,*kkey,*val;
		std::unordered_map<rObj*,rObj*,Hash,Equal> umap;
		if ((type = rdbLoadType(rdb)) == -1)
			return	REDIS_ERR;
		
		if (type == REDIS_RDB_HSET)
			return REDIS_OK;
		
		if ((key = rdbLoadStringObject(rdb)) == nullptr)
			return	REDIS_ERR;

		if ((len = rdbLoadLen(rdb,nullptr)) == -1)
			return	REDIS_ERR;
		
		
		for(int i = 0 ; i < len; i ++)
		{
			if ((rdbver = rdbLoadType(rdb)) == -1)
				return	REDIS_ERR;
			
			if(rdbver != REDIS_RDB_TYPE_STRING)
				return REDIS_ERR;
				
			if ((kkey = rdbLoadStringObject(rdb)) == nullptr)
				return	REDIS_ERR;
	  
			if ((val = rdbLoadObject(rdbver,rdb)) == nullptr)
				return	REDIS_ERR;

			kkey->calHash();
			umap.insert(std::make_pair(kkey,val));
		}

		key->calHash();
		size_t hash = key->hash;
		MutexLock &mu = redis->hsetMapShards[hash% redis->kShards].mutex;
		auto & hsetMap = redis->hsetMapShards[hash % redis->kShards].hsetMap;
		MutexLockGuard lock(mu);
		auto it = hsetMap.find(key);
		if(it == hsetMap.end())
		{
			hsetMap.insert(std::make_pair(key,std::move(umap)));
		}
		else
		{
			zfree(it->first);
			for(auto iter = it->second.begin(); iter != it->second.end(); iter++)
			{
				zfree(iter->first);
				zfree(iter->second);
			}
			it->second.clear();
			hsetMap.erase(it);
			hsetMap.insert(std::make_pair(key,std::move(umap)));
		}
		
	}

	return REDIS_OK;
}

rObj  * rdbLoad(char *filename,size_t &len)
{
	xRio rdb;
	FILE *fp ;
	if((fp = fopen(filename,"r")) == nullptr)
	{
		return nullptr;
	}
	rioInitWithFile(&rdb,fp);
	struct stat sb;
	if(fstat(fileno(fp),&sb) == -1)
	{
		return nullptr;
	}
	else
	{
		LOG_INFO<<"load dump.rdb size :"<<sb.st_size;
	}

	rObj * o = createStringObject(NULL,sb.st_size);
	len = sdsllen(o->ptr);
	if(rioRead(&rdb,(void*)o->ptr,sb.st_size) == 0)
	{
		zfree(o);
		return nullptr;
	}
		
	return o;

}

int  rdbWrite(char *filename,const char *buf, size_t len)
{
	FILE *fp ;
	xRio rdb;
	char tmpfile[256];
	snprintf(tmpfile,256,"temp-%d.rdb",(int)getpid());
	fp = fopen(tmpfile,"w");
	if(!fp)
	{
		 LOG_TRACE<<"Failed opening .rdb for saving:"<<strerror(errno);
		 return REDIS_ERR;
	}

	rioInitWithFile(&rdb,fp);
	if(rioWrite(&rdb,buf,len) == 0)
	{
		return REDIS_ERR;
	}

	if (fflush(fp) == EOF) return REDIS_ERR;
	if (fsync(fileno(fp)) == -1) return REDIS_ERR;
	if (fclose(fp) == EOF) return REDIS_ERR;

	if(rename(tmpfile,filename) == -1)
	{
		return REDIS_ERR;
	}
	
	return REDIS_OK;
}


int rdbLoad(char *filename,xRedis * redis)
{
	uint32_t dbid;
	int type,rdbver;
	char buf[1024];
	FILE *fp ;
	xRio rdb;

	if((fp = fopen(filename,"r")) == nullptr)
	{
		return REDIS_ERR;
	}
	rioInitWithFile(&rdb,fp);
	if(rioRead(&rdb,buf,9) == 0)
	{
		return REDIS_ERR;
	}
	buf[9] = '\0';

	if(memcmp(buf,"REDIS",5) !=0)
	{
		fclose(fp);
		LOG_ERROR<<"Wrong signature trying to load DB from file";
		return REDIS_ERR;
	}

	struct stat sb;
	if(fstat(fileno(fp),&sb) == -1)
	{	
		return REDIS_ERR;
	}
	else
	{
		LOG_INFO<<"load dump.rdb "<<sb.st_size;
	}

	if(rdbLoadSet(&rdb,redis) != REDIS_OK )
	{
		return REDIS_ERR;
	}
	
	if(rdbLoadHset(&rdb,redis) != REDIS_OK)
	{
		return REDIS_ERR;
	}
	

	uint64_t cksum,expected = rdb.cksum;
	if(rioRead(&rdb,&cksum,8) == 0)
	{
		return REDIS_ERR;
	}
	
	if (cksum == 0)
	{
		LOG_WARN<<"RDB file was saved with checksum disabled: no check performed";
		return REDIS_ERR;
	}
	else if(cksum != expected)
	{
		LOG_WARN<<"Wrong RDB checksum. Aborting now";
		return REDIS_ERR;
	}

	fclose(fp);
	return REDIS_OK;
}


int rdbLoadType(xRio *rdb) 
{
    unsigned char type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}


uint32_t rdbLoadUType(xRio *rdb) 
{
    uint32_t  type;
    if (rioRead(rdb,&type,1) == 0) return -1;
    return type;
}




int rdbSaveLen(xRio *rdb, uint32_t len)
{
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1<<6)) {
        /* Save a 6 bit len */
        buf[0] = (len&0xFF)|(REDIS_RDB_6BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1<<14)) {
        /* Save a 14 bit len */
        buf[0] = ((len>>8)&0xFF)|(REDIS_RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
        if (rdbWriteRaw(rdb,buf,2) == -1) return -1;
        nwritten = 2;
    } else {
        /* Save a 32 bit len */
        buf[0] = (REDIS_RDB_32BITLEN<<6);
        if (rdbWriteRaw(rdb,buf,1) == -1) return -1;
        len = htonl(len);
        if (rdbWriteRaw(rdb,&len,4) == -1) return -1;
        nwritten = 1+4;
    }
    return nwritten;
}


int rdbSaveLzfStringObject(xRio *rdb, unsigned char *s, size_t len)
{
	size_t comprlen,outlen;
	unsigned char byte;
	int n,nwritten = 0;
	void *out;

	if(len <=4 ) return 0;
	outlen = len- 4;
	if((out = zmalloc(outlen + 1)) == nullptr) return 0;
	comprlen = lzf_compress(s,len,out,outlen);
	if(comprlen == 0)
	{
		zfree(out);
		return 0;
	}
	
	/* Data compressed! Let's save it on disk */
    byte = (REDIS_RDB_ENCVAL<<6)|REDIS_RDB_ENC_LZF;
    if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) return REDIS_ERR;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,comprlen)) == -1) return REDIS_ERR;
    nwritten += n;

    if ((n = rdbSaveLen(rdb,len)) == -1) return REDIS_ERR;
    nwritten += n;

    if ((n = rdbWriteRaw(rdb,out,comprlen)) == -1) return REDIS_ERR;
    nwritten += n;

    zfree(out);
    return nwritten;
	
}

size_t rdbSaveRawString(xRio *rdb, const  char *s, size_t len)
{
	int enclen;
	int n,nwritten = 0;

	if( len > 20)
	{
		n = rdbSaveLzfStringObject(rdb, (unsigned char*)s,len);
		if (n == -1) return -1;
      	if (n > 0) return n;
	}
	
	if((n = rdbSaveLen(rdb,len) == -1))
	{
		return -1;
	}
	nwritten += n;
	if(len > 0)
	{
		if (rdbWriteRaw(rdb,(void*)s,len) == -1) 
		{
			return -1;	
		}
		
        nwritten += len;
	}
	
	return nwritten;
	
}



rObj *rdbLoadObject(int rdbtype, xRio *rdb) 
{
	rObj *o = nullptr;
	size_t len;
	unsigned int i;
	if(rdbtype == REDIS_RDB_TYPE_STRING)
	{
		if ((o = rdbLoadEncodedStringObject(rdb)) == nullptr)
		{
			return nullptr;	
		}
	}
	else
	{
		LOG_WARN<<"rdbLoadObject type error";
	}
	return o;
}

int rdbSaveStringObject(xRio *rdb, rObj *obj)
{
	return rdbSaveRawString(rdb,obj->ptr,sdsllen(obj->ptr));
}


int rdbSaveType(xRio *rdb, unsigned char type)
{
    return rdbWriteRaw(rdb,&type,1);
}



int rdbSaveObjectType(xRio *rdb, rObj *o)
{
	switch(o->type)
	{
		case REDIS_STRING:
		{
			return rdbSaveType(rdb,REDIS_RDB_TYPE_STRING);
		}
	}
	return 1;
}	


int rdbSaveObject(xRio *rdb, rObj *o)
{
	int n, nwritten = 0;

	if (o->type == REDIS_STRING)
	{
		if ((n = rdbSaveStringObject(rdb,o)) == -1)
		{
			return -1;
		}
		nwritten += n;
    } 
    
    return nwritten;
}

	

int rdbSaveKeyValuePair(xRio *rdb, rObj *key, rObj *val, long long now)
{
    if (rdbSaveObjectType(rdb,val) == -1) return -1;
    if (rdbSaveStringObject(rdb,key) == -1) return -1;
    if (rdbSaveObject(rdb,val) == -1) return -1;
	return 1;
}


int rdbSaveValue(xRio *rdb, rObj *value,long long now)
{
    if (rdbSaveObjectType(rdb,value) == -1) return -1;
    if (rdbSaveStringObject(rdb,value) == -1) return -1;

	return 1;
}



int rdbSaveKey(xRio *rdb, rObj *key,long long now)
{
    if (rdbSaveObjectType(rdb,key) == -1) return -1;
    if (rdbSaveStringObject(rdb,key) == -1) return -1;
	return 1;
}



int rdbWriteRaw(xRio *rdb, void *p, size_t len)
{
   if (rdb && rioWrite(rdb,p,len) == 0)
   { 
   	 return -1;
   }
	
   return len;
}


int rdbSaveRio(xRio *rdb,int *error,xRedis * redis)
{
	char magic[10];
	int j;
	long long now = time(0);
	uint64_t cksum;
	snprintf(magic,sizeof(magic),"REDIS%04d",REDIS_RDB_VERSION);
	if (rdbWriteRaw(rdb,magic,9) == -1)
	{
		*error = errno;
		return REDIS_ERR;
	}

	rdbSaveSet(rdb,redis);
	rdbSaveHset(rdb,redis);
	cksum = rdb->cksum;
	if(rioWrite(rdb,&cksum,8) == 0)
	{
		*error = errno;
		return REDIS_ERR;
	}
	
	return REDIS_OK;
}
int rdbSave(char *filename,xRedis * redis)
{
	char tmpfile[256];
	FILE *fp;
	xRio rdb;
	int error;

	snprintf(tmpfile,256,"temp-%d.rdb",(int)getpid());
	fp = fopen(tmpfile,"w");
	if(!fp)
	{
		 LOG_TRACE<<"Failed opening .rdb for saving:"<<strerror(errno);
		 return REDIS_ERR;
	}

	rioInitWithFile(&rdb,fp);
	if(rdbSaveRio(&rdb,&error,redis) == REDIS_ERR)
	{
		errno = error;
		goto werr;
	}

	if (fflush(fp) == EOF) goto werr;
	if (fsync(fileno(fp)) == -1) goto werr;
	if (fclose(fp) == EOF) goto werr;

	if(rename(tmpfile,filename) == -1)
	{
		LOG_TRACE<<"Error moving temp DB file  on the final:"<<strerror(errno);
		unlink(tmpfile);
		return REDIS_ERR;
	}

	return REDIS_OK;
werr:
	 LOG_WARN<<"Write error saving DB on disk:" <<strerror(errno);
	 fclose(fp);
	 unlink(tmpfile);
	 return REDIS_ERR;
}
