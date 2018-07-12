#pragma once

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <deque>
#include <limits>
#include <set>
#include <string_view>
#include <memory>
#include <vector>
#include "status.h"

static const size_t kBufSize = 65536;

static bool startsWith(const std::string_view &x,const std::string_view &y)
{
	return ((x.size() >= y.size()) && (memcmp(x.data(),y.data(),x.size()) == 0));
}

static Status posixError(const std::string &context,int err)
{
	if (err == ENOENT)
	{
		return Status::notFound(context,strerror(err));
	}
	else
	{
		return Status::ioError(context,strerror(err));
	}
}

class PosixWritableFile
{
private:
	// buf_[0, pos_-1] contains data to be written to fd_.
	std::string filename;
	int fd;
	char buf[kBufSize];
	size_t pos;
public:
	PosixWritableFile(const std::string &fname,int fd);
	~PosixWritableFile();

    Status append(const std::string_view &data);
	Status close();
	Status flush();
	Status syncDirIfManifest();
	Status sync();

private:
	Status flushBuffered();
	Status writeRaw(const char *p,size_t n);
};

class PosixEnv
{
public:
	bool fileExists(const std::string &fname);
	Status newWritableFile(const std::string &fname,std::shared_ptr<PosixWritableFile> &result);
	Status deleteFile(const std::string &fname);
	Status createDir(const std::string &name);
	Status deleteDir(const std::string &name);
	Status getFileSize(const std::string &fname,uint64_t *size);
	Status getChildren(const std::string &dir,std::vector<std::string> *result);
};






