#include "xRio.h"


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
	r->maxProcessingChunk = 0;
	r->io.file.fp = fp;
	r->io.file.buffered = 0;
	r->io.file.autosync = 0;
}

void rioInitWithBuffer(xRio *r, sds s)
{

}

