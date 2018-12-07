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
#include <mutex>
#include <thread>
#include "status.h"

static const size_t kBufSize = 65536;

static bool startsWith(const std::string_view &x, const std::string_view &y)
{
	return ((x.size() >= y.size()) && (memcmp(x.data(), y.data(), y.size()) == 0));
}

static Status posixError(const std::string &context, int err)
{
	if (err == ENOENT)
	{
		return Status::notFound(context, strerror(err));
	}
	else
	{
		return Status::ioError(context, strerror(err));
	}
}

static int LockOrUnlock(int fd, bool lock) 
{
	errno = 0;
	struct ::flock file_lock_info;
	memset(&file_lock_info, 0, sizeof(file_lock_info));
	file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
	file_lock_info.l_whence = SEEK_SET;
	file_lock_info.l_start = 0;
	file_lock_info.l_len = 0;  // Lock/unlock entire file.
	return ::fcntl(fd, F_SETLK, &file_lock_info);
}


// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class WritableFile
{
public:
	WritableFile() = default;

	WritableFile(const WritableFile&) = delete;
	WritableFile &operator=(const WritableFile&) = delete;

	virtual ~WritableFile() { }

	virtual Status append(const std::string_view &data) = 0;
	virtual Status close() = 0;
	virtual Status flush() = 0;
	virtual Status sync() = 0;
};

// Log the specified data to *info_log if info_log is non-null.
//void Log(Logger *infoLog,const char *format,...);
class PosixWritableFile : public WritableFile
{
private:
	// buf_[0, pos_-1] contains data to be written to fd_.
	std::string filename;
	int fd;
	char buf[kBufSize]; 
	size_t pos;
public:
	PosixWritableFile(const std::string &fname, int fd);
	~PosixWritableFile();

	virtual Status append(const std::string_view &data);
	virtual Status close();
	virtual Status flush();
	virtual Status syncDirIfManifest();
	virtual Status sync();

private:
	Status flushBuffered();
	Status writeRaw(const char *p, size_t n);
};

// A file abstraction for randomly reading the contents of a file.
class RandomAccessFile 
{
public:
	RandomAccessFile() = default;

	RandomAccessFile(const RandomAccessFile&) = delete;
	RandomAccessFile& operator=(const RandomAccessFile&) = delete;

	virtual ~RandomAccessFile() { }

	// Read up to "n" bytes from the file starting at "offset".
	// "scratch[0..n-1]" may be written by this routine.  Sets "*result"
	// to the data that was read (including if fewer than "n" bytes were
	// successfully read).  May set "*result" to point at data in
	// "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
	// "*result" is used.  If an error was encountered, returns a non-OK
	// status.
	//
	// Safe for concurrent use by multiple threads.
	virtual Status read(uint64_t offset, size_t n, std::string_view *result,
	              char *scratch) const = 0;
};

// pread() based random-access
class PosixRandomAccessFile : public RandomAccessFile
{
private:
	std::string filename;
	int fd;
public:
	PosixRandomAccessFile(const std::string &fname, int fd);
	virtual ~PosixRandomAccessFile();
	virtual Status read(uint64_t offset, size_t n, std::string_view *result,
		char *scratch) const;
};

// mmap() based random-access
class PosixMmapReadableFile : public RandomAccessFile
{
private:
	std::string filename;
	char *const mmapBase;
	size_t length;

public:
	// base[0,length-1] contains the mmapped contents of the file.
	PosixMmapReadableFile(const std::string &fname, char *base, size_t length);
	virtual ~PosixMmapReadableFile();
	virtual Status read(uint64_t offset, size_t n, std::string_view *result,
		char *scratch) const;
};

class PosixSequentialFile
{
private:
	std::string filename;
	int fd;
public:
	PosixSequentialFile(const std::string &fname, int fd);
	~PosixSequentialFile();

	Status read(size_t n, std::string_view *result, char *scratch);
	Status skip(uint64_t n);
};

class PosixLockTable 
{
public:
	bool Insert(const std::string &fname)
	{
		std::unique_lock<std::mutex> lk(mutex);
		bool succeeded = lockedFiles.insert(fname).second;
		return succeeded;
	}
	void remove(const std::string &fname) 
	{
		std::unique_lock<std::mutex> lk(mutex);
		lockedFiles.erase(fname);
	}

private:
	std::mutex mutex;
	std::set<std::string> lockedFiles;
};


// Instances are thread-safe because they are immutable.
class PosixFileLock
{
public:
	PosixFileLock(int fd, std::string filename)
	  : fd(fd), filename(std::move(filename)) { }

	int getfd() const { return fd; }
	const std::string &getfilename() const { return filename; }

	private:
	const int fd;
	const std::string filename;
};

class PosixEnv
{
public:
	// Returns true iff the named file exists.
	bool fileExists(const std::string &fname);
	// Create an object that writes to a new file with the specified
	// name.  Deletes any existing file with the same name and creates a
	// new file.  On success, stores a pointer to the new file in
	// *result and returns OK.  On failure stores nullptr in *result and
	// returns non-OK.
	//
	// The returned file will only be accessed by one thread at a time.

	Status newWritableFile(const std::string &fname, std::shared_ptr<PosixWritableFile> &result);
	// Delete the named file.
	Status deleteFile(const std::string &fname);
	// Create the specified directory.
	Status createDir(const std::string &name);
	// Delete the specified directory.
	Status deleteDir(const std::string &name);
	// Store the size of fname in *file_size.
	Status getFileSize(const std::string &fname, uint64_t *size);

	// Store in *result the names of the children of the specified directory.
	// The names are relative to "dir".
	// Original contents of *results are dropped.
	Status getChildren(const std::string &dir, std::vector<std::string> *result);
	// Create a brand new sequentially-readable file with the specified name.
	// On success, stores a pointer to the new file in *result and returns OK.
	// On failure stores nullptr in *result and returns non-OK.  If the file does
	// not exist, returns a non-OK status.  Implementations should return a
	// NotFound status when the file does not exist.
	//
	// The returned file will only be accessed by one thread at a time.

	// May create the named file if it does not already exist.
	Status lockFile(const std::string& fname, std::shared_ptr<PosixFileLock> &lock);
	// Release the lock acquired by a previous successful call to LockFile.
	// REQUIRES: lock was returned by a successful LockFile() call
	// REQUIRES: lock has not already been unlocked.
	Status unlockFile(const std::shared_ptr<PosixFileLock> &lock);
  
	Status newSequentialFile(const std::string &fname,
		std::shared_ptr<PosixSequentialFile> &result);
	// Rename file src to target.
	Status renameFile(const std::string &src, const std::string &target);

	// Create an object that either appends to an existing file, or
  // writes to a new file (if the file does not exist to begin with).
  // On success, stores a pointer to the new file in *result and
  // returns OK.  On failure stores nullptr in *result and returns
  // non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  //
  // May return an IsNotSupportedError error if this Env does
  // not allow appending to an existing file.  Users of Env (including
  // the leveldb implementation) must be prepared to deal with
  // an Env that does not support appending.
	Status newAppendableFile(const std::string &fname,
		std::shared_ptr<PosixWritableFile> &result);

	// Returns the number of micro-seconds since some fixed point in time. Only
	// useful for computing deltas of time.
	uint64_t nowMicros();

	// Create an object supporting random-access reads from the file with the
	// specified name.  On success, stores a pointer to the new file in
	// *result and returns OK.  On failure stores nullptr in *result and
	// returns non-OK.  If the file does not exist, returns a non-OK
	// status.  Implementations should return a NotFound status when the file does
	// not exist.
	//
	// The returned file may be concurrently accessed by multiple threads.
	Status newRandomAccessFile(const std::string &filename,
								 std::shared_ptr<RandomAccessFile> &result);
};










