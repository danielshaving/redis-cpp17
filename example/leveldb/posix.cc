#include "posix.h"

PosixWritableFile::PosixWritableFile(const std::string &fname,int fd)
:filename(fname),fd(fd),pos(0)
{

}

PosixWritableFile::~PosixWritableFile()
{
	if (fd >= 0)
	{
		close();
	}
}

Status PosixWritableFile::append(const std::string_view &data)
{
	size_t n = data.size();
	const char *p = data.data();

	// Fit as much as possible into buffer.
	size_t copy = std::min(n,kBufSize - pos);
	memcpy(buf + pos,p,copy);
	p += copy;
	n -= copy;
	pos += copy;
	if (n == 0)
	{
		Status s;
		return s;
	}

	// Can't fit in buffer, so need to do at least one write.
	Status s = flushBuffered();
	if (!s.ok())
	{
		return s;
	}

	// Small writes go to buffer, large writes are written directly.
	if (n < kBufSize)
	{
		memcpy(buf,p,n);
		pos = n;
		Status s;
		return s;
	}
	return writeRaw(p,n);
}

Status PosixWritableFile::close()
{
	Status result = flushBuffered();
	const int r = ::close(fd);
	if (r < 0 && result.ok())
	{
		result = posixError(filename,errno);
	}
	fd = -1;
	return result;
}

Status PosixWritableFile::flush()
{
	return flushBuffered();
}


Status PosixWritableFile::syncDirIfManifest()
{
	const char *f = filename.c_str();
	const char *sep = strrchr(f,'/');
	std::string_view basename;
	std::string dir;
	if (sep == nullptr)
	{
		dir = ".";
		basename = f;
	}
	else
	{
		dir = std::string(f,sep - f);
		basename = sep + 1;
	}

	Status s;
	if (startsWith(basename,"MANIFEST"))
	{
		int fd = ::open(dir.c_str(),O_RDONLY);
		if (fd < 0)
		{
			s = posixError(dir,errno);
		}
		else
		{
			if (fsync(fd) < 0)
			{
				s = posixError(dir,errno);
			}
			::close(fd);
		}
	}
	return s;
}

Status PosixWritableFile::sync()
{
	// Ensure new files referred to by the manifest are in the filesystem.
	Status s = syncDirIfManifest();
	if (!s.ok())
	{
		return s;
	}

	s = flushBuffered();
	if (s.ok())
	{
		if (::fsync(fd) != 0)
		{
			s = posixError(filename,errno);
		}
	}
	return s;
}

Status PosixWritableFile::flushBuffered()
{
	Status s = writeRaw(buf,pos);
	pos = 0;
	return s;
}

Status PosixWritableFile::writeRaw(const char *p,size_t n)
{
	while (n > 0)
	{
		ssize_t r = ::write(fd,p,n);
		if (r < 0)
		{
			if (errno == EINTR)
			{
				continue;  // Retry
			}
			return posixError(filename,errno);
		}
		p += r;
		n -= r;
	}
	Status s;
	return s;
}

PosixSequentialFile::PosixSequentialFile(const std::string &fname,int fd)
  : filename(fname),fd(fd)
{

}

PosixSequentialFile::~PosixSequentialFile()
{
	::close(fd);
}

Status PosixSequentialFile::read(size_t n,std::string_view *result,char *scratch)
{
	Status s;
	while (true)
	{
		ssize_t r = ::read(fd,scratch,n);
		if (r < 0)
		{
			if (errno == EINTR)
			{
				continue;  // Retry
			}

			s = posixError(filename,errno);
			break;
		}

		*result = std::string_view(scratch,r);
		break;
	}
	return s;
}

Status PosixSequentialFile::skip(uint64_t n)
{
	Status s;
	if (::lseek(fd,n,SEEK_CUR) == static_cast<off_t>(-1))
	{
		return posixError(filename,errno);
	}
	return s;
};


Status PosixEnv::newWritableFile(const std::string &fname,std::shared_ptr<PosixWritableFile> &result)
 {
	Status s;
	int fd = ::open(fname.c_str(),O_TRUNC | O_WRONLY | O_CREAT,0644);
	if (fd < 0) 
	{
		s = posixError(fname,errno);
	}
	else
	{
		result = std::shared_ptr<PosixWritableFile>(new PosixWritableFile(fname,fd));
	}
	return s;
}

Status PosixEnv::deleteFile(const std::string &fname)
{
	Status result;
	if (::unlink(fname.c_str()) != 0)
	{
		result = posixError(fname,errno);
	}
	return result;
}

Status PosixEnv::createDir(const std::string &name)
{
	Status result;
	if (::mkdir(name.c_str(),0755) != 0)
	{
		result = posixError(name,errno);
	}
	return result;
}

Status PosixEnv::deleteDir(const std::string &name)
{
	Status result;
	if (::rmdir(name.c_str()) != 0)
	{
		result = posixError(name,errno);
	}
	return result;
}

Status PosixEnv::getFileSize(const std::string &fname,uint64_t* size)
{
	Status s;
	struct stat sbuf;
	if (::stat(fname.c_str(),&sbuf) != 0)
	{
		*size = 0;
		s = posixError(fname,errno);
	}
	else
	{
		*size = sbuf.st_size;
	}
	return s;
}

bool PosixEnv::fileExists(const std::string &fname)
{
	return access(fname.c_str(),F_OK) == 0;
}

Status PosixEnv::getChildren(const std::string &dir,std::vector<std::string> *result)
{
	Status s;
	result->clear();
	DIR *d = ::opendir(dir.c_str());
	if (d == nullptr)
	{
		return posixError(dir,errno);
	}

	struct dirent *entry;
	while ((entry = ::readdir(d)) != nullptr)
	{
		result->push_back(entry->d_name);
	}
	::closedir(d);
	return s;
}

Status PosixEnv::newSequentialFile(const std::string &fname,std::shared_ptr<PosixSequentialFile> &result)
{
	Status s;
	int fd = open(fname.c_str(),O_RDONLY);
	if (fd < 0)
	{
		result = nullptr;
		return posixError(fname,errno);
	}
	else
	{
		result = std::shared_ptr<PosixSequentialFile>(new PosixSequentialFile(fname,fd));
		return s;
	}
}





