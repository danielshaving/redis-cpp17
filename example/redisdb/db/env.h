#pragma once
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <type_traits>
#include <utility>
#include <fcntl.h>
#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <exception>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <queue>
#include <set>
#include <thread>
#include <type_traits>
#include <utility>
#include <iostream> 
#include <inttypes.h>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <vector>
#include <list>
#include <map>
#include <sys/types.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <functional>
#include <algorithm>
#include <memory>
#include <condition_variable>
#include <thread>
#include <sys/types.h>
#include <signal.h>
#include <string>
#include <iosfwd>
#include <string>
#include <errno.h>
#include <array>
#include <utility>
#include <limits.h>
#include <stdint.h>
#include <sys/stat.h>
#include <atomic>
#include <stdarg.h>
#include <limits.h>
#include <any>
#include <string_view>
#include <ratio>
#include <chrono>
#include <random>
#include <cstring>

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <deque>
#include <limits>
#include <string_view>
#include <memory>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>

#include "status.h"
#include "util.h"

static const size_t kWritableFileBufferSize = 65536;

// Up to 1000 mmaps for 64-bit binaries; none for 32-bit.
static int kDefaultMmapLimit = sizeof(void*) >= 8 ? 1000 : 0;

static const size_t kBufSize = 65536;

// Helper class to limit resource usage to avoid exhaustion.
// Currently used to limit read-only file descriptors and mmap file usage
// so that we do not run out of file descriptors or virtual memory, or run into
// kernel performance problems for very large databases.
class Limiter {
public:
	// Limit maximum number of resources to |max_acquires|.
	Limiter(int acquires) : acquiresAllowed(acquires) {}

	Limiter(const Limiter&) = delete;
	Limiter operator=(const Limiter&) = delete;

	// If another resource is available, Acquire it and return true.
	// Else return false.
	bool Acquire() {
		int oldacquiresAllowed =
			acquiresAllowed.fetch_sub(1, std::memory_order_relaxed);

		if (oldacquiresAllowed > 0)
			return true;

		acquiresAllowed.fetch_add(1, std::memory_order_relaxed);
		return false;
	}

	// Release a resource acquired by a previous call to Acquire() that returned
	// true.
	void Release() {
		acquiresAllowed.fetch_add(1, std::memory_order_relaxed);
	}

private:
	// The number of available resources.
	//
	// This is a counter and is not tied to the invariants of any other class, so
	// it can be operated on safely using std::memory_order_relaxed.
	std::atomic<int> acquiresAllowed;
};

// A file abstraction for randomly reading the Contents of a file.
class RandomAccessFile {
public:
	RandomAccessFile() = default;

	RandomAccessFile(const RandomAccessFile&) = delete;
	RandomAccessFile& operator=(const RandomAccessFile&) = delete;

	virtual ~RandomAccessFile() {

	}

	// Read up to "n" bytes from the file starting at "offset".
	// "scratch[0..n-1]" may be written by this routine.  Sets "*result"
	// to the data that was read (including if fewer than "n" bytes were
	// successfully read).  May Set "*result" to point at data in
	// "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
	// "*result" is used.  If an error was encountered, returns a non-OK
	// status.
	//
	// Safe for concurrent use by multiple threads.
	virtual Status read(uint64_t offset, size_t n, std::string_view* result,
		char* scratch) const = 0;
};

static Status PosixError(const std::string & context, int error) {
	if (error == ENOENT) {
		return Status::NotFound(context, std::strerror(error));
	}
	else {
		return Status::IOError(context, std::strerror(error));
	}
}


// Implements sequential read access in a file using read().
//
// Instances of this class are thread-friendly but not thread-safe, as required
// by the SequentialFile API.
class SequentialFile {
public:
	SequentialFile(std::string filename, int fd)
		: fd(fd), filename(filename) {}
	~SequentialFile() { close(fd); }

	Status read(size_t n, std::string_view* result, char* scratch) {
		Status status;
		while (true) {
			::ssize_t readSize = ::read(fd, scratch, n);
			if (readSize < 0) {  // Read error.
				if (errno == EINTR) {
					continue;  // Retry
				}
				status = PosixError(filename, errno);
				break;
			}
			*result = std::string_view(scratch, readSize);
			break;
		}
		return status;
	}

	Status Skip(uint64_t n) {
		if (::lseek(fd, n, SEEK_CUR) == static_cast<off_t>(-1)) {
			return PosixError(filename, errno);
		}
		return Status::OK();
	}

private:
	const int fd;
	const std::string filename;
};

// Implements random read access in a file using pread().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class PosixRandomAccessFile : public RandomAccessFile {
public:
	// The new instance takes ownership of |fd|. |fd_limiter| must outlive this
	// instance, and will be used to determine if .
	PosixRandomAccessFile(std::string filename, int fd, Limiter* limiter)
		: has(limiter->Acquire()),
		fd(has ? fd : -1),
		limiter(limiter),
		filename(std::move(filename)) {
		if (!has) {
			assert(fd == -1);
			::close(fd);  // The file will be opened on every read.
		}
	}

	~PosixRandomAccessFile() {
		if (has) {
			assert(fd != -1);
			::close(fd);
			limiter->Release();
		}
	}

	Status read(uint64_t offset, size_t n, std::string_view * result,
		char* scratch) const {

		int ffd;
		if (!has) {
			ffd = ::open(filename.c_str(), O_RDONLY);
			if (ffd < 0) {
				return PosixError(filename, errno);
			}
		}

		assert(ffd != -1);

		Status status;
		ssize_t readSize = ::pread(ffd, scratch, n, static_cast<off_t>(offset));
		*result = std::string_view(scratch, (readSize < 0) ? 0 : readSize);
		if (readSize < 0) {
			// An error: return a non-ok status.
			status = PosixError(filename, errno);
		}

		if (!has) {
			// Close the temporary file descriptor opened earlier.
			assert(fd != ffd);
			::close(ffd);
		}
		return status;
	}

private:
	const bool has;  // If false, the file is opened on every read.
	const int fd;  // -1 if has_permanent_fd_ is false.
	Limiter* const limiter;
	const std::string filename;
};

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class PosixMmapReadableFile : public RandomAccessFile {
public:
	// mmap_base[0, length-1] points to the memory-mapped Contents of the file. It
	// must be the result of a successful call to mmap(). This instances takes
	// over the ownership of the region.
	//
	// |mmap_limiter| must outlive this instance. The caller must have already
	// aquired the right to use one mmap region, which will be released when this
	// instance is destroyed.
	PosixMmapReadableFile(std::string filename, char* mmapbase, size_t length,
		Limiter* limiter)
		: mmapbase(mmapbase), length(length), limiter(limiter),
		filename(std::move(filename)) {}

	~PosixMmapReadableFile() {
		::munmap(static_cast<void*>(mmapbase), length);
		limiter->Release();
	}

	Status read(uint64_t offset, size_t n, std::string_view* result,
		char* scratch) const {
		if (offset + n > length) {
			*result = std::string_view();
			return PosixError(filename, EINVAL);
		}

		*result = std::string_view(mmapbase + offset, n);
		return Status::OK();
	}

private:
	char* const mmapbase;
	const size_t length;
	Limiter* const limiter;
	const std::string filename;
};

class WritableFile {
public:
	WritableFile(std::string filename, int fd)
		: pos(0), fd(fd), manifest(isManifest(filename)),
		filename(std::move(filename)), dirname(DirName(filename)) {}

	~WritableFile() {
		if (fd >= 0) {
			// Ignoring any potential errors
			close();
		}
	}

	Status append(const std::string_view& data) {
		size_t writeSize = data.size();
		const char* writeData = data.data();

		// Fit as much as possible into buffer.
		size_t copySize = std::min(writeSize, kWritableFileBufferSize - pos);
		std::memcpy(buf + pos, writeData, copySize);
		writeData += copySize;
		writeSize -= copySize;
		pos += copySize;
		if (writeSize == 0) {
			return Status::OK();
		}

		// Can't fit in buffer, so need to do at least one Write.
		Status status = FlushBuffer();
		if (!status.ok()) {
			return status;
		}

		// Small writes go to buffer, large writes are written directly.
		if (writeSize < kWritableFileBufferSize) {
			std::memcpy(buf, writeData, writeSize);
			pos = writeSize;
			return Status::OK();
		}
		return WriteUnbuffered(writeData, writeSize);
	}

	Status close() {
		Status status = FlushBuffer();
		const int result = ::close(fd);
		if (result < 0 && status.ok()) {
			status = PosixError(filename, errno);
		}

		fd = -1;
		return status;
	}

	Status flush() {
		return FlushBuffer();
	}

	Status sync() {
		// Ensure new files referred to by the manifest are in the filesystem.
		//
		// This needs to happen before the manifest file is flushed to disk, to
		// avoid crashing in a state where the manifest refers to files that are not
		// yet on disk.
		Status status = SyncDirIfManifest();
		if (!status.ok()) {
			return status;
		}

		status = FlushBuffer();
		if (!status.ok()) {
			return status;
		}

		return SyncFd(fd, filename);
	}

private:
	Status FlushBuffer() {
		Status status = WriteUnbuffered(buf, pos);
		pos = 0;
		return status;
	}

	Status WriteUnbuffered(const char* data, size_t size) {
		while (size > 0) {
			ssize_t result = ::write(fd, data, size);
			if (result < 0) {
				if (errno == EINTR) {
					continue;  // Retry
				}
				return PosixError(filename, errno);
			}

			data += result;
			size -= result;
		}
		return Status::OK();
	}

	Status SyncDirIfManifest() {
		Status status;
		if (!manifest) {
			return status;
		}

		int fd = ::open(dirname.c_str(), O_RDONLY);
		if (fd < 0) {
			status = PosixError(dirname, errno);
		}
		else {
			status = SyncFd(fd, dirname);
			::close(fd);
		}
		return status;
	}

	// Ensures that all the caches associated with the given file descriptor's
	// data are flushed all the way to durable media, and can withstand power
	// failures.
	//
	// The path argument is only used to populate the description string in the
	// returned Status if an error occurs.
	static Status SyncFd(int fd, const std::string& path) {
#if HAVE_FULLFSYNC
		// On macOS and iOS, fsync() doesn't guarantee durability past power
		// failures. fcntl(F_FULLFSYNC) is required for that purpose. Some
		// filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to
		// fsync().
		if (::fcntl(fd, F_FULLFSYNC) == 0) {
			return Status::OK();
		}
#endif  // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
		bool sync = ::fdatasync(fd) == 0;
#else
		bool sync = ::fsync(fd) == 0;
#endif  // HAVE_FDATASYNC

		if (sync) {
			return Status::OK();
		}
		return PosixError(path, errno);
	}

	// Returns the directory name in a path pointing to a file.
	//
	// Returns "." if the path does not contain any directory separator.
	static std::string DirName(const std::string& filename) {
		std::string::size_type pos = filename.rfind('/');
		if (pos == std::string::npos) {
			return std::string(".");
		}
		// The filename component should not contain a path separator. If it does,
		// the splitting was done incorrectly.
		assert(filename.find('/', pos + 1) == std::string::npos);

		return filename.substr(0, pos);
	}

	// Extracts the file name from a path pointing to a file.
	//
	// The returned Slice points to |filename|'s data buffer, so it is only Valid
	// while |filename| is alive and unchanged.
	static std::string_view Basename(const std::string & filename) {
		std::string::size_type pos = filename.rfind('/');
		if (pos == std::string::npos) {
			return std::string_view(filename);
		}
		// The filename component should not contain a path separator. If it does,
		// the splitting was done incorrectly.
		assert(filename.find('/', pos + 1) == std::string::npos);

		return std::string_view(filename.data() + pos + 1,
			filename.length() - pos - 1);
	}

	// True if the given file is a manifest file.
	static bool isManifest(const std::string & filename) {
		return StartsWith(Basename(filename), "MANIFEST");
	}

	// buf_[0, pos_ - 1] Contains data to be written to fd_.
	char buf[kWritableFileBufferSize];
	size_t pos;
	int fd;

	const bool manifest;  // True if the file's name starts with MANIFEST.
	const std::string filename;
	const std::string dirname;  // The directory of filename_.
};

static int LockOrUnlock(int fd, bool lock) {
	errno = 0;
	struct ::flock info;
	std::memset(&info, 0, sizeof(info));
	info.l_type = (lock ? F_WRLCK : F_UNLCK);
	info.l_whence = SEEK_SET;
	info.l_start = 0;
	info.l_len = 0;  // Lock/unlock entire file.
	return ::fcntl(fd, F_SETLK, &info);
}

// Instances are thread-safe because they are immutable.
class FileLock {
public:
	FileLock(int fd, std::string filename)
		: fd(fd), filename(std::move(filename)) { }

	int getfd() const { return fd; }
	const std::string& getfilename() const { return filename; }

private:
	const int fd;
	const std::string filename;
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate Set instead of relying on fcntrl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
class LockTable {
public:
	bool insert(const std::string& fname) {
		std::unique_lock<std::mutex> lck(mutex);
		bool succeeded = lockedFiles.insert(fname).second;
		return succeeded;
	}

	void remove(const std::string& fname) {
		std::unique_lock<std::mutex> lck(mutex);
		lockedFiles.erase(fname);
	}

private:
	std::mutex mutex;
	std::set<std::string> lockedFiles;
};

// Return the maximum number of read-only files to keep Open.
static int MaxOpenFiles() {
	int limit;
	struct ::rlimit rlim;
	if (::getrlimit(RLIMIT_NOFILE, &rlim)) {
		// getrlimit failed, fallback to hard-coded default.
		limit = 50;
	}
	else if (rlim.rlim_cur == RLIM_INFINITY) {
		limit = std::numeric_limits<int>::max();
	}
	else {
		// Allow use of 20% of available file descriptors for read-only files.
		limit = rlim.rlim_cur / 5;
	}
	return limit;
}

class Logger;

class Env {
public:
	typedef std::function<void()> Functor;

	Env()
		:limiter(kDefaultMmapLimit),
		fdlimiter(MaxOpenFiles()),
		startbgthread(false),
		shuttingdown(false) {
			
	}

	~Env() {

	}

	Status NewSequentialFile(const std::string& filename,
		std::shared_ptr<SequentialFile>& result) {
		int fd = ::open(filename.c_str(), O_RDONLY);
		if (fd < 0) {
			result = nullptr;
			return PosixError(filename, errno);
		}

		result.reset(new SequentialFile(filename, fd));
		return Status::OK();
	}

	Status NewRandomAccessFile(const std::string& filename,
		std::shared_ptr<RandomAccessFile>& result) {
		result = nullptr;
		int fd = ::open(filename.c_str(), O_RDONLY);
		if (fd < 0) {
			return PosixError(filename, errno);
		}

		if (!limiter.Acquire()) {
			result.reset(new PosixRandomAccessFile(filename, fd, &fdlimiter));
			return Status::OK();
		}

		uint64_t filesize;
		Status status = GetFileSize(filename, &filesize);
		if (status.ok()) {
			void* base = ::mmap(/*addr=*/nullptr, filesize, PROT_READ,
				MAP_SHARED, fd, 0);
			if (base != MAP_FAILED) {
				result.reset(new PosixMmapReadableFile(
					filename, reinterpret_cast<char*>(base), filesize, &limiter));
			}
			else {
				status = PosixError(filename, errno);
			}
		}

		::close(fd);
		if (!status.ok()) {
			limiter.Release();
		}
		return status;
	}

	Status NewWritableFile(const std::string& filename,
		std::shared_ptr<WritableFile>& result) {
		int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
		if (fd < 0) {
			result = nullptr;
			return PosixError(filename, errno);
		}

		result.reset(new WritableFile(filename, fd));
		return Status::OK();
	}

	Status NewAppendableFile(const std::string& filename,
		std::shared_ptr<WritableFile>& result) {
		int fd = ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
		if (fd < 0) {
			result = nullptr;
			return PosixError(filename, errno);
		}

		result.reset(new WritableFile(filename, fd));
		return Status::OK();
	}

	bool FileExists(const std::string& filename) {
		return ::access(filename.c_str(), F_OK) == 0;
	}

	Status GetChildren(const std::string& path,
		std::vector<std::string>* result) {
		result->clear();
		::DIR* dir = ::opendir(path.c_str());
		if (dir == nullptr) {
			return PosixError(path, errno);
		}

		struct ::dirent* entry;
		while ((entry = ::readdir(dir)) != nullptr) {
			result->emplace_back(entry->d_name);
		}
		::closedir(dir);
		return Status::OK();
	}

	Status DeleteFile(const std::string& filename) {
		if (::unlink(filename.c_str()) != 0) {
			return PosixError(filename, errno);
		}
		return Status::OK();
	}

	Status CreateDir(const std::string& dirname) {
		if (::mkdir(dirname.c_str(), 0755) != 0) {
			return PosixError(dirname, errno);
		}
		return Status::OK();
	}

	Status DeleteDir(const std::string& dirname) {
		if (::rmdir(dirname.c_str()) != 0) {
			return PosixError(dirname, errno);
		}
		return Status::OK();
	}

	Status GetFileSize(const std::string& filename, uint64_t* size) {
		struct ::stat fileStat;
		if (::stat(filename.c_str(), &fileStat) != 0) {
			*size = 0;
			return PosixError(filename, errno);
		}

		*size = fileStat.st_size;
		return Status::OK();
	}

	Status RenameFile(const std::string& from, const std::string& to) {
		if (std::rename(from.c_str(), to.c_str()) != 0) {
			return PosixError(from, errno);
		}
		return Status::OK();
	}

	Status LockFile(const std::string& filename, std::shared_ptr<FileLock>& lock) {
		lock = nullptr;

		int fd = ::open(filename.c_str(), O_RDWR | O_CREAT, 0644);
		if (fd < 0) {
			return PosixError(filename, errno);
		}

		if (!locks.insert(filename)) {
			::close(fd);
			return Status::IOError("lock " + filename, "already held by process");
		}

		if (LockOrUnlock(fd, true) == -1) {
			int lockerrno = errno;
			::close(fd);
			locks.remove(filename);
			return PosixError("lock " + filename, lockerrno);
		}

		lock.reset(new FileLock(fd, filename));
		return Status::OK();
	}

	Status UnlockFile(const std::shared_ptr<FileLock>& lock) {
		if (LockOrUnlock(lock->getfd(), false) == -1) {
			return PosixError("unlock " + lock->getfilename(), errno);
		}

		locks.remove(lock->getfilename());
		::close(lock->getfd());
		return Status::OK();
	}

	Status GetTestDirectory(std::string* result) {
		const char* env = std::getenv("TEST_TMPDIR");
		if (env && env[0] != '\0') {
			*result = env;
		}
		else {
			char buf[100];
			std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d",
				static_cast<int>(::geteuid()));
			*result = buf;
		}

		// The CreateDir status is ignored because the directory may already exist.
		CreateDir(*result);
		return Status::OK();
	}

	uint64_t NowMicros() {
		static constexpr uint64_t kUsecondsPerSecond = 1000000;
		struct ::timeval tv;
		::gettimeofday(&tv, nullptr);
		return static_cast<uint64_t>(tv.tv_sec)* kUsecondsPerSecond + tv.tv_usec;
	}

	Status NewLogger(const std::string& fname, std::shared_ptr<Logger>& result);

	void SleepForMicroseconds(int micros) {
		std::this_thread::sleep_for(std::chrono::microseconds(micros));
	}

	void ExitSchedule() {
		if (startbgthread) {
			shuttingdown = true;
			Schedule(Functor());
			if (bgthread->joinable()) {
				bgthread->join();
			}
			startbgthread = false;
		}
	}

	void Schedule(Functor&& func) {
		// Start the background thread, if we haven't done so already.
		std::unique_lock<std::mutex> lk(bgmutex);
		if (!startbgthread) {
			startbgthread = true;
			bgthread.reset(new std::thread(std::bind(&Env::BackgroundThreadMain, this)));
		}
		
		// If the queue is empty, the background thread may be waiting for work.
		bgqueue.emplace_back(func);
		bgcond.notify_one();
	}
private:
	void BackgroundThreadMain() {
		while (!shuttingdown) {
			std::unique_lock<std::mutex> lk(bgmutex);

			// Wait until there is work to be done.
			while (bgqueue.empty()) {
				bgcond.wait(lk);
			}

			assert(!bgqueue.empty());
			auto func = bgqueue.front();
			bgqueue.pop_front();
			if (func) {
				func();
			}
		}
	}

private:
	LockTable locks;  
	Limiter limiter;  
	Limiter fdlimiter;

	std::atomic<bool> shuttingdown;
	std::atomic<bool> startbgthread;
	std::mutex bgmutex;
	std::condition_variable bgcond;
	std::deque<Functor> bgqueue;
	std::shared_ptr<std::thread> bgthread;
};

enum InfoLogLevel {
	DEBUG_LEVEL = 0,
	INFO_LEVEL,
	WARN_LEVEL,
	ERROR_LEVEL,
	FATAL_LEVEL,
	HEADER_LEVEL,
	NUM_INFO_LOG_LEVELS,
};

// An interface for writing log messages.
class Logger {
public:
	size_t kDoNotSupportGetLogFileSize = (std::numeric_limits<size_t>::max)();

	explicit Logger(const InfoLogLevel level = InfoLogLevel::INFO_LEVEL)
		: closed(false), loglevel(level) {}
	virtual ~Logger();

	// Close the log file. Must be called before destructor. If the return
	// status is NotSupported(), it means the implementation does cleanup in
	// the destructor
	virtual Status close();

	// Write a header to the log file with the specified format
	// It is recommended that you log all header information at the start of the
	// application. But it is not enforced.
	virtual void LogHeader(const char* format, va_list ap) {
		// Default implementation does a simple INFO level log Write.
		// Please override as per the logger class requirement.
		Logv(format, ap);
	}

	// Write an entry to the log file with the specified format.
	virtual void Logv(const char* format, va_list ap) = 0;

	// Write an entry to the log file with the specified log level
	// and format.  Any log with level under the internal log level
	// of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
	// printed.
	virtual void Logv(const InfoLogLevel log_level, const char* format, va_list ap);

	virtual size_t GetLogFileSize() const { return kDoNotSupportGetLogFileSize; }
	// Flush to the OS buffers
	virtual void flush() {}

	virtual InfoLogLevel GetInfoLogLevel() const { return loglevel; }

	virtual void SetInfoLogLevel(const InfoLogLevel level) {
		loglevel = level;
	}

protected:
	virtual Status CloseImpl();
	bool closed;

private:
	// No copying allowed
	Logger(const Logger&);
	void operator=(const Logger&);
	InfoLogLevel loglevel;
};

extern void LogFlush(const std::shared_ptr<Logger>& infolog);

extern void Log(const InfoLogLevel level,
	const std::shared_ptr<Logger>& infolog, const char* format,
	...);

// a Set of log functions with different log levels.
extern void Header(const std::shared_ptr<Logger>& infolog, const char* format,
	...);
extern void Debug(const std::shared_ptr<Logger>& infolog, const char* format,
	...);
extern void Info(const std::shared_ptr<Logger>& infolog, const char* format,
	...);
extern void Warn(const std::shared_ptr<Logger>& infolog, const char* format,
	...);
extern void Error(const std::shared_ptr<Logger>& infolog, const char* format,
	...);
extern void Fatal(const std::shared_ptr<Logger>& infolog, const char* format,
	...);

// Log the specified data to *infolog if infolog is non-nullptr.
// The default info log level is InfoLogLevel::INFO_LEVEL.
extern void Log(const std::shared_ptr<Logger>& infolog, const char* format,
	...)
#   if defined(__GNUC__) || defined(__clang__)
	__attribute__((__format__(__printf__, 2, 3)))
#   endif
	;

extern void LogFlush(Logger* infolog);

extern void Log(const InfoLogLevel log_level, Logger* infolog,
	const char* format, ...);

// The default info log level is InfoLogLevel::INFO_LEVEL.
extern void Log(Logger* infolog, const char* format, ...)
#   if defined(__GNUC__) || defined(__clang__)
__attribute__((__format__(__printf__, 2, 3)))
#   endif
;

// a Set of log functions with different log levels.
extern void Header(Logger* infolog, const char* format, ...);
extern void Debug(Logger* infolog, const char* format, ...);
extern void Info(Logger* infolog, const char* format, ...);
extern void Warn(Logger* infolog, const char* format, ...);
extern void Error(Logger* infolog, const char* format, ...);
extern void Fatal(Logger* infolog, const char* format, ...);





