#pragma once

#ifdef _WIN64
#include <windows.h>
#else
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
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#endif


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
#include <set>
#include <string_view>
#include <memory>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include "logger.h"
#include "status.h"

static const size_t kWritableFileBufferSize = 65536;

// Up to 1000 mmaps for 64-bit binaries; none for 32-bit.
static int kDefaultMmapLimit = sizeof(void*) >= 8 ? 1000 : 0;

static const size_t kBufSize = 65536;
static bool StartsWith(const std::string_view &x, const std::string_view &y) {
	return ((x.size() >= y.size()) && (memcmp(x.data(), y.data(), y.size()) == 0));
}


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

	// If another resource is available, acquire it and return true.
	// Else return false.
	bool acquire() {
		int oldacquiresAllowed =
			acquiresAllowed.fetch_sub(1, std::memory_order_relaxed);

		if (oldacquiresAllowed > 0)
			return true;

		acquiresAllowed.fetch_add(1, std::memory_order_relaxed);
		return false;
	}

	// Release a resource acquired by a previous call to Acquire() that returned
	// true.
	void release() {
		acquiresAllowed.fetch_add(1, std::memory_order_relaxed);
	}

private:
	// The number of available resources.
	//
	// This is a counter and is not tied to the invariants of any other class, so
	// it can be operated on safely using std::memory_order_relaxed.
	std::atomic <int> acquiresAllowed;
};

#ifdef _WIN64
static std::string GetWindowsErrorMessage(DWORD code) {
	std::string message;
	char *error = nullptr;
	// Use MBCS version of FormatMessage to match return value.
	size_t size = ::FormatMessageA(
	  FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_ALLOCATE_BUFFER |
		  FORMAT_MESSAGE_IGNORE_INSERTS,
	  nullptr, code, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
	  reinterpret_cast<char*>(&error), 0, nullptr);
	if (!error) {
		return message;
	}
	
	message.assign(error, size);
	::LocalFree(error);
	return message;
}

static Status WindowsError(const std::string &context, DWORD code) {
	if (code == ERROR_FILE_NOT_FOUND || code == ERROR_PATH_NOT_FOUND)
		return Status::notFound(context, GetWindowsErrorMessage(code));
	return Status::ioError(context, GetWindowsErrorMessage(code));
}

class ScopedHandle {
public:
	ScopedHandle(HANDLE handle) : handle(handle) {}
	ScopedHandle(ScopedHandle &&other) noexcept : handle(other.release()) {}
	~ScopedHandle() { close(); }

	ScopedHandle &operator=(ScopedHandle &&rhs) noexcept {
		if (this != &rhs) handle = rhs.release();
		return *this;
	}

	bool close() {
		if (!valid()) {
			return true;
		}
		
		HANDLE h = handle;
		handle = INVALID_HANDLE_VALUE;
		return ::CloseHandle(h);
	}

	bool valid() const {
		return handle != INVALID_HANDLE_VALUE && handle != nullptr;
	}

	HANDLE get() const { return handle; }

	HANDLE release() {
		HANDLE h = handle;
		handle = INVALID_HANDLE_VALUE;
		return h;
	}

private:
	HANDLE handle;
};

static bool lockOrUnlock(HANDLE handle, bool lock) {
	if (lock) {
		return ::LockFile(handle,
					  /*dwFileOffsetLow=*/0, /*dwFileOffsetHigh=*/0,
					  /*nNumberOfBytesToLockLow=*/MAXDWORD,
					  /*nNumberOfBytesToLockHigh=*/MAXDWORD);
	} else {
		return ::UnlockFile(handle,
						/*dwFileOffsetLow=*/0, /*dwFileOffsetHigh=*/0,
						/*nNumberOfBytesToLockLow=*/MAXDWORD,
						/*nNumberOfBytesToLockHigh=*/MAXDWORD);
	}
}

class SequentialFile {
public:
	SequentialFile(std::string fname, ScopedHandle file)
	  : filename(fname), file(std::move(file)) {}
	~SequentialFile() {}

	Status read(size_t n, std::string_view *result, char *scratch) {
		Status s;
		DWORD bytesRead;
		// DWORD is 32-bit, but size_t could technically be larger. However leveldb
		// files are limited to leveldb::Options::max_file_size which is clamped to
		// 1<<30 or 1 GiB.
		assert(n <= (std::numeric_limits<DWORD>::max)());
		if (!::ReadFile(file.get(), scratch, static_cast<DWORD>(n), &bytesRead,
						nullptr)) {
		  s = WindowsError(filename, ::GetLastError());
		} else {
		  *result = std::string_view(scratch, bytesRead);
		}
		return s;
	}

	Status skip(uint64_t n) {
		LARGE_INTEGER distance;
		distance.QuadPart = n;
		if (!::SetFilePointerEx(file.get(), distance, nullptr, FILE_CURRENT)) {
			return WindowsError(filename, ::GetLastError());
		}
		return Status::OK();
	}

private:
	std::string filename;
	ScopedHandle file;
};

class RandomAccessFile {
public:
	RandomAccessFile(std::string fname, ScopedHandle handle)
	  : filename(fname), handle(std::move(handle)) {}

	~RandomAccessFile() {

	}

	Status read(uint64_t offset, size_t n, std::string_view *result,
			char *scratch) const {
		DWORD bytesRead = 0;
		OVERLAPPED overlapped = {0};

		overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
		overlapped.Offset = static_cast<DWORD>(offset);
		if (!::ReadFile(handle.get(), scratch, static_cast<DWORD>(n), &bytesRead,
						&overlapped)) {
			DWORD code = ::GetLastError();
			if (code != ERROR_HANDLE_EOF) {
				*result = std::string_view(scratch, 0);
				return Status::ioError(filename, GetWindowsErrorMessage(code));
			}
		}

		*result = std::string_view(scratch, bytesRead);
		return Status::OK();
	}

private:
	std::string filename;
	ScopedHandle handle;
};

class MMapReadableFile {
public:
	// base[0,length-1] contains the mmapped contents of the file.
	MMapReadableFile(std::string fname, void* base, size_t length)
		: filename(std::move(fname)),
		mmappedregion(base),
		length(length) {

	}

	~MMapReadableFile() {
		::UnmapViewOfFile(mmappedregion);
	}

	Status read(uint64_t offset, size_t n, std::string_view *result,
			  char *scratch) const {
		Status s;
		if (offset + n > length) {
			*result = std::string_view();
			s = WindowsError(filename, ERROR_INVALID_PARAMETER);
		} else {
		  *result = std::string_view(reinterpret_cast<char*>(mmappedregion) + offset, n);
		}
		return s;
	}

private:
	std::string filename;
	void *mmappedregion;
	size_t length;
};

class WritableFile {
public:
	WritableFile(std::string fname, ScopedHandle handle) :
		filename(std::move(fname)), handle(std::move(handle)), pos(0) {}

	~WritableFile() { }

	Status append(const std::string_view &data) {
		size_t n = data.size();
		const char *p = data.data();

		// Fit as much as possible into buffer.

		size_t copy = min(n, kWritableFileBufferSize - pos);

		memcpy(buf + pos, p, copy);
		p += copy;
		n -= copy;
		pos += copy;
		if (n == 0) {
			return Status::OK();
		}

		// Can't fit in buffer, so need to do at least one write.
		Status s = flushBuffered();
		if (!s.ok()) {
			return s;
		}

		// Small writes go to buffer, large writes are written directly.
		if (n < kWritableFileBufferSize) {
			memcpy(buf, p, n);
			pos = n;
			return Status::OK();
		}
		return writeRaw(p, n);
	}

	Status close() {
		Status result = flushBuffered();
		if (!handle.close() && result.ok()) {
			result = WindowsError(filename, ::GetLastError());
		}
		return result;
	}

	Status flush() { return flushBuffered(); }

	Status sync() {
		// On Windows no need to sync parent directory. It's metadata will be
		// updated via the creation of the new file, without an explicit sync.
		return flushBuffered();
	}

private:
	Status flushBuffered() {
		Status s = writeRaw(buf, pos);
		pos = 0;
		return s;
	}

	Status writeRaw(const char *p, size_t n) {
		DWORD bytesWritten;
		if (!::WriteFile(handle.get(), p, static_cast<DWORD>(n), &bytesWritten,
						 nullptr)) {
		  return Status::ioError(filename,
								 GetWindowsErrorMessage(::GetLastError()));
		}
		return Status::OK();
	}

	// buf_[0, pos_-1] contains data to be written to handle_.
	const std::string filename;
	ScopedHandle handle;
	char buf[kWritableFileBufferSize];
	size_t pos;
};

class FileLock {
public:
	FileLock(ScopedHandle handle, std::string name)
		: handle(std::move(handle)), name(std::move(name)) {}

	ScopedHandle &gethandle() { return handle; }
	const std::string &getname() const { return name; }

private:
	ScopedHandle handle;
	std::string name;
};

class Env {
public:
	Env() {

	}

	~Env() {
	
	}

	Status newSequentialFile(const std::string &fname,
		std::shared_ptr <SequentialFile> &result) {
		result = nullptr;
		DWORD access = GENERIC_READ;
		DWORD mode = FILE_SHARE_READ;
		ScopedHandle handle =
			::CreateFileA(fname.c_str(), access, mode, nullptr,
				OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
		if (!handle.valid()) {
			return WindowsError(fname, ::GetLastError());
		}

		result.reset(new SequentialFile(fname, std::move(handle)));
		return Status::OK();
	}

	Status newRandomAccessFile(const std::string &fname,
		std::shared_ptr <MMapReadableFile> &result) {
		result = nullptr;
		DWORD access = GENERIC_READ;
		DWORD mode = FILE_SHARE_READ;
		DWORD falgs = FILE_ATTRIBUTE_READONLY;

		ScopedHandle handle =
			::CreateFileA(fname.c_str(), access, mode, nullptr,
				OPEN_EXISTING, falgs, nullptr);
		if (!handle.valid()) {
			return WindowsError(fname, ::GetLastError());
		}

		
		//result.reset(new RandomAccessFile(fname, std::move(handle)));
		//return Status::OK();

		LARGE_INTEGER fileSize;
		if (!::GetFileSizeEx(handle.get(), &fileSize)) {
			return WindowsError(fname, ::GetLastError());
		}

		ScopedHandle mapping =
			::CreateFileMappingA(handle.get(),
				/*security attributes=*/nullptr, PAGE_READONLY,
				/*dwMaximumSizeHigh=*/0,
				/*dwMaximumSizeLow=*/0, nullptr);
		if (mapping.valid()) {
			void* base = MapViewOfFile(mapping.get(), FILE_MAP_READ, 0, 0, 0);
			if (base) {
				result.reset(new MMapReadableFile(
					fname, base, static_cast<size_t>(fileSize.QuadPart)));
				return Status::OK();
			}
		}

		Status s = WindowsError(fname, ::GetLastError());
		return s;
	}

	Status newWritableFile(const std::string& fname,
		std::shared_ptr <WritableFile> &result) {
		DWORD desired_access = GENERIC_WRITE;
		DWORD share_mode = 0;

		ScopedHandle handle =
			::CreateFileA(fname.c_str(), desired_access, share_mode, nullptr,
				CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
		if (!handle.valid()) {
			result = nullptr;
			return WindowsError(fname, ::GetLastError());
		}

		result.reset(new WritableFile(fname, std::move(handle)));
		return Status::OK();
	}

	Status newAppendableFile(const std::string &fname,
		std::shared_ptr <WritableFile> &result) {
		ScopedHandle handle =
			::CreateFileA(fname.c_str(), FILE_APPEND_DATA, 0, nullptr, OPEN_ALWAYS,
				FILE_ATTRIBUTE_NORMAL, nullptr);
		if (!handle.valid()) {
			result = nullptr;
			return WindowsError(fname, ::GetLastError());
		}

		result.reset(new WritableFile(fname, std::move(handle)));
		return Status::OK();
	}

	bool fileExists(const std::string &fname) {
		return GetFileAttributesA(fname.c_str()) != INVALID_FILE_ATTRIBUTES;
	}

	Status getChildren(const std::string &dir,
		std::vector <std::string> *result) {
		const std::string pattern = dir + "\\*";
		WIN32_FIND_DATAA findData;
		HANDLE dir_handle = ::FindFirstFileA(pattern.c_str(), &findData);
		if (dir_handle == INVALID_HANDLE_VALUE) {
			DWORD error = ::GetLastError();
			if (error == ERROR_FILE_NOT_FOUND) {
				return Status::OK();
			}
			return WindowsError(dir, error);
		}
		do {
			char base_name[_MAX_FNAME];
			char ext[_MAX_EXT];

			if (!_splitpath_s(findData.cFileName, nullptr, 0, nullptr, 0, base_name,
				ARRAYSIZE(base_name), ext, ARRAYSIZE(ext))) {
				result->emplace_back(std::string(base_name) + ext);
			}
		} while (::FindNextFileA(dir_handle, &findData));

		DWORD error = ::GetLastError();
		::FindClose(dir_handle);
		if (error != ERROR_NO_MORE_FILES) {
			return WindowsError(dir, error);
		}
		return Status::OK();
	}

	Status deleteFile(const std::string & fname) {
		if (!::DeleteFileA(fname.c_str())) {
			return WindowsError(fname, ::GetLastError());
		}
		return Status::OK();
	}

	Status createDir(const std::string &name) {
		if (!::CreateDirectoryA(name.c_str(), nullptr)) {
			return WindowsError(name, ::GetLastError());
		}
		return Status::OK();
	}

	Status deleteDir(const std::string &name) {
		if (!::RemoveDirectoryA(name.c_str())) {
			return WindowsError(name, ::GetLastError());
		}
		return Status::OK();
	}

	Status getFileSize(const std::string &fname, uint64_t *size) {
		WIN32_FILE_ATTRIBUTE_DATA attrs;
		if (!::GetFileAttributesExA(fname.c_str(), GetFileExInfoStandard, &attrs)) {
			return WindowsError(fname, ::GetLastError());
		}

		ULARGE_INTEGER file_size;
		file_size.HighPart = attrs.nFileSizeHigh;
		file_size.LowPart = attrs.nFileSizeLow;
		*size = file_size.QuadPart;
		return Status::OK();
	}

	Status renameFile(const std::string &src,
		const std::string &target) {
		// Try a simple move first.  It will only succeed when |to_path| doesn't
		// already exist.
		if (::MoveFileA(src.c_str(), target.c_str())) {
			return Status::OK();
		}
		DWORD merror = ::GetLastError();

		// Try the full-blown replace if the move fails, as ReplaceFile will only
		// succeed when |to_path| does exist. When writing to a network share, we
		// may not be able to change the ACLs. Ignore ACL errors then
		// (REPLACEFILE_IGNORE_MERGE_ERRORS).
		if (::ReplaceFileA(target.c_str(), src.c_str(), nullptr,
			REPLACEFILE_IGNORE_MERGE_ERRORS, nullptr, nullptr)) {
			return Status::OK();
		}
		DWORD error = ::GetLastError();
		// In the case of FILE_ERROR_NOT_FOUND from ReplaceFile, it is likely
		// that |to_path| does not exist. In this case, the more relevant error
		// comes from the call to MoveFile.
		if (error == ERROR_FILE_NOT_FOUND ||
			error == ERROR_PATH_NOT_FOUND) {
			return WindowsError(src, merror);
		}
		else {
			return WindowsError(src, error);
		}
	}

	Status getTestDirectory(std::string *result) {
		const char *env = getenv("TEST_TMPDIR");
		if (env && env[0] != '\0') {
			*result = env;
			return Status::OK();
		}

		char path[MAX_PATH];
		if (!GetTempPathA(ARRAYSIZE(path), path)) {
			return WindowsError("GetTempPath", ::GetLastError());
		}
		std::stringstream ss;
		ss << path << "leveldbtest-" << std::this_thread::get_id();
		*result = ss.str();

		// Directory may already exist
		createDir(*result);
		return Status::OK();
	}

	Status newLogger(const std::string & filename, std::shared_ptr <Logger> &result) {
		std::FILE *fp = std::fopen(filename.c_str(), "w");
		if (fp == nullptr) {
			result = nullptr;
			return WindowsError("NewLogger", ::GetLastError());
		}
		else {
			result.reset(new Logger(fp));
			return Status::OK();
		}
	}

	uint64_t nowMicros() {
		// GetSystemTimeAsFileTime typically has a resolution of 10-20 msec.
		// TODO(cmumford): Switch to GetSystemTimePreciseAsFileTime which is
		// available in Windows 8 and later.
		FILETIME ft;
		::GetSystemTimeAsFileTime(&ft);
		// Each tick represents a 100-nanosecond intervals since January 1, 1601
		// (UTC).
		uint64_t ticks =
			(static_cast<uint64_t>(ft.dwHighDateTime) << 32) + ft.dwLowDateTime;
		return ticks / 10;
	}

	void sleepForMicroseconds(int micros) {
		std::this_thread::sleep_for(std::chrono::microseconds(micros));
	}
};
#else 

static Status PosixError(const std::string &context, int error) {
	if (error == ENOENT) {
		return Status::notFound(context, std::strerror(error));
	}
	else {
		return Status::ioError(context, std::strerror(error));
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

	Status read(size_t n, std::string_view *result, char *scratch) {
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

	Status skip(uint64_t n) {
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
class RandomAccessFile {
public:
	// The new instance takes ownership of |fd|. |fd_limiter| must outlive this
	// instance, and will be used to determine if .
	RandomAccessFile(std::string filename, int fd, Limiter *limiter)
		: has(limiter->acquire()),
		fd(has ? fd : -1),
		limiter(limiter),
		filename(std::move(filename)) {
		if (!has) {
			assert(fd == -1);
			::close(fd);  // The file will be opened on every read.
		}
	}

	~RandomAccessFile() {
		if (has) {
			assert(fd != -1);
			::close(fd);
			limiter->release();
		}
	}

	Status read(uint64_t offset, size_t n, std::string_view *result,
		char *scratch) const {

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
	Limiter *const limiter;
	const std::string filename;
};

// Implements random read access in a file using mmap().
//
// Instances of this class are thread-safe, as required by the RandomAccessFile
// API. Instances are immutable and Read() only calls thread-safe library
// functions.
class MMapReadableFile {
public:
	// mmap_base[0, length-1] points to the memory-mapped contents of the file. It
	// must be the result of a successful call to mmap(). This instances takes
	// over the ownership of the region.
	//
	// |mmap_limiter| must outlive this instance. The caller must have already
	// aquired the right to use one mmap region, which will be released when this
	// instance is destroyed.
	MMapReadableFile(std::string filename, char *mmapbase, size_t length,
		Limiter *limiter)
		: mmapbase(mmapbase), length(length), limiter(limiter),
		filename(std::move(filename)) {}

	~MMapReadableFile() {
		::munmap(static_cast<void*>(mmapbase), length);
		limiter->release();
	}

	Status read(uint64_t offset, size_t n, std::string_view *result,
		char *scratch) const {
		if (offset + n > length) {
			*result = std::string_view();
			return PosixError(filename, EINVAL);
		}

		*result = std::string_view(mmapbase + offset, n);
		return Status::OK();
	}

private:
	char *const mmapbase;
	const size_t length;
	Limiter *const limiter;
	const std::string filename;
};

class WritableFile {
public:
	WritableFile(std::string filename, int fd)
		: pos(0), fd(fd), manifest(isManifest(filename)),
		filename(std::move(filename)), dirname(dirName(filename)) {}

	~WritableFile() {
		if (fd >= 0) {
			// Ignoring any potential errors
			close();
		}
	}

	Status append(const std::string_view &data) {
		size_t writeSize = data.size();
		const char *writeData = data.data();

		// Fit as much as possible into buffer.
		size_t copySize = std::min(writeSize, kWritableFileBufferSize - pos);
		std::memcpy(buf + pos, writeData, copySize);
		writeData += copySize;
		writeSize -= copySize;
		pos += copySize;
		if (writeSize == 0) {
			return Status::OK();
		}

		// Can't fit in buffer, so need to do at least one write.
		Status status = flushBuffer();
		if (!status.ok()) {
			return status;
		}

		// Small writes go to buffer, large writes are written directly.
		if (writeSize < kWritableFileBufferSize) {
			std::memcpy(buf, writeData, writeSize);
			pos = writeSize;
			return Status::OK();
		}
		return writeUnbuffered(writeData, writeSize);
	}

	Status close() {
		Status status = flushBuffer();
		const int result = ::close(fd);
		if (result < 0 && status.ok()) {
			status = PosixError(filename, errno);
		}

		fd = -1;
		return status;
	}

	Status flush() {
		return flushBuffer();
	}

	Status sync() {
		// Ensure new files referred to by the manifest are in the filesystem.
		//
		// This needs to happen before the manifest file is flushed to disk, to
		// avoid crashing in a state where the manifest refers to files that are not
		// yet on disk.
		Status status = syncDirIfManifest();
		if (!status.ok()) {
			return status;
		}

		status = flushBuffer();
		if (!status.ok()) {
			return status;
		}

		return syncFd(fd, filename);
  }

private:
	Status flushBuffer() {
		Status status = writeUnbuffered(buf, pos);
		pos = 0;
		return status;
	}

	Status writeUnbuffered(const char *data, size_t size) {
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

	Status syncDirIfManifest() {
		Status status;
		if (!manifest) {
			return status;
		}

		int fd = ::open(dirname.c_str(), O_RDONLY);
		if (fd < 0) {
			status = PosixError(dirname, errno);
		}
		else {
			status = syncFd(fd, dirname);
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
	static Status syncFd(int fd, const std::string &path) {
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
	static std::string dirName(const std::string &filename) {
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
	// The returned Slice points to |filename|'s data buffer, so it is only valid
	// while |filename| is alive and unchanged.
	static std::string_view basename(const std::string &filename) {
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
	static bool isManifest(const std::string &filename) {
		return StartsWith(basename(filename), "MANIFEST");
	}

	// buf_[0, pos_ - 1] contains data to be written to fd_.
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
		: fd(fd), filename(std::move(filename)) {}

	int getfd() const { return fd; }
	const std::string &getfilename() const { return filename; }

private:
	const int fd;
	const std::string filename;
};

// Tracks the files locked by PosixEnv::LockFile().
//
// We maintain a separate set instead of relying on fcntrl(F_SETLK) because
// fcntl(F_SETLK) does not provide any protection against multiple uses from the
// same process.
//
// Instances are thread-safe because all member data is guarded by a mutex.
class LockTable {
public:
	bool insert(const std::string &fname) {
		std::unique_lock <std::mutex> lck(mutex);
		bool succeeded = lockedFiles.insert(fname).second;
		return succeeded;
	}

	void remove(const std::string& fname) {
		std::unique_lock <std::mutex> lck(mutex);
		lockedFiles.erase(fname);
	}

private:
	std::mutex mutex;
	std::set <std::string> lockedFiles;
};

// Return the maximum number of read-only files to keep open.
static int maxOpenFiles() {
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

class Env {
public:
	Env(): limiter(kDefaultMmapLimit), fdlimiter(maxOpenFiles()) {

	}

	~Env() {

	}

	Status newSequentialFile(const std::string &filename,
		std::shared_ptr <SequentialFile> &result) {
		int fd = ::open(filename.c_str(), O_RDONLY);
		if (fd < 0) {
			result = nullptr;
			return PosixError(filename, errno);
		}

		result.reset(new SequentialFile(filename, fd));
		return Status::OK();
	}

	Status newRandomAccessFile(const std::string &filename,
		std::shared_ptr <MMapReadableFile> &result) {
		result = nullptr;
		int fd = ::open(filename.c_str(), O_RDONLY);
		if (fd < 0) {
			return PosixError(filename, errno);
		}

		/*
		if (!limiter.acquire()) {
			result.reset(new RandomAccessFile(filename, fd, &fdlimiter));
			return Status::OK();
		}
		*/

		uint64_t fileSize;
		Status status = getFileSize(filename, &fileSize);
		if (status.ok()) {
			void *base = ::mmap(/*addr=*/nullptr, fileSize, PROT_READ,
				MAP_SHARED, fd, 0);
			if (base != MAP_FAILED) {
				result.reset(new MMapReadableFile(
					filename, reinterpret_cast<char*>(base), fileSize, &limiter));
			}
			else {
				status = PosixError(filename, errno);
			}
		}
		::close(fd);
		if (!status.ok()) {
			limiter.release();
		}
		return status;
	}

	Status newWritableFile(const std::string &filename,
		std::shared_ptr <WritableFile> &result) {
		int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
		if (fd < 0) {
			result = nullptr;
			return PosixError(filename, errno);
		}

		result.reset(new WritableFile(filename, fd));
		return Status::OK();
	}

	Status newAppendableFile(const std::string &filename,
		std::shared_ptr <WritableFile> &result) {
		int fd = ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
		if (fd < 0) {
			result = nullptr;
			return PosixError(filename, errno);
		}

		result.reset(new WritableFile(filename, fd));
		return Status::OK();
	}

	bool fileExists(const std::string &filename) {
		return ::access(filename.c_str(), F_OK) == 0;
	}

	Status getChildren(const std::string &path,
		std::vector <std::string> *result) {
		result->clear();
		::DIR *dir = ::opendir(path.c_str());
		if (dir == nullptr) {
			return PosixError(path, errno);
		}

		struct ::dirent *entry;
		while ((entry = ::readdir(dir)) != nullptr) {
			result->emplace_back(entry->d_name);
		}
		::closedir(dir);
		return Status::OK();
	}

	Status deleteFile(const std::string &filename) {
		if (::unlink(filename.c_str()) != 0) {
			return PosixError(filename, errno);
		}
		return Status::OK();
	}

	Status createDir(const std::string& dirname) {
		if (::mkdir(dirname.c_str(), 0755) != 0) {
			return PosixError(dirname, errno);
		}
		return Status::OK();
	}

	Status deleteDir(const std::string &dirname) {
		if (::rmdir(dirname.c_str()) != 0) {
			return PosixError(dirname, errno);
		}
		return Status::OK();
	}

	Status getFileSize(const std::string &filename, uint64_t *size) {
		struct ::stat fileStat;
		if (::stat(filename.c_str(), &fileStat) != 0) {
			*size = 0;
			return PosixError(filename, errno);
		}

		*size = fileStat.st_size;
		return Status::OK();
	}

	Status renameFile(const std::string &from, const std::string &to) {
		if (std::rename(from.c_str(), to.c_str()) != 0) {
			return PosixError(from, errno);
		}
		return Status::OK();
	}

	Status lockFile(const std::string& filename, std::shared_ptr <FileLock> &lock) {
		lock = nullptr;

		int fd = ::open(filename.c_str(), O_RDWR | O_CREAT, 0644);
		if (fd < 0) {
			return PosixError(filename, errno);
		}

		if (!locks.insert(filename)) {
			::close(fd);
			return Status::ioError("lock " + filename, "already held by process");
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

	Status unlockFile(const std::shared_ptr <FileLock> &lock) {
		if (LockOrUnlock(lock->getfd(), false) == -1) {
			return PosixError("unlock " + lock->getfilename(), errno);
		}

		locks.remove(lock->getfilename());
		::close(lock->getfd());
		return Status::OK();
	}

	Status getTestDirectory(std::string* result) {
		const char *env = std::getenv("TEST_TMPDIR");
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
		createDir(*result);

		return Status::OK();
	}

	Status newLogger(const std::string& filename, std::shared_ptr <Logger> &result) {
		std::FILE *fp = std::fopen(filename.c_str(), "w");
		if (fp == nullptr) {
			result = nullptr;
			return PosixError(filename, errno);
		}
		else {
			result.reset(new Logger(fp));
			return Status::OK();
		}
	}

	uint64_t nowMicros() {
		static constexpr uint64_t kUsecondsPerSecond = 1000000;
		struct ::timeval tv;
		::gettimeofday(&tv, nullptr);
		return static_cast<uint64_t>(tv.tv_sec)* kUsecondsPerSecond + tv.tv_usec;
	}

	void sleepForMicroseconds(int micros) {
		std::this_thread::sleep_for(std::chrono::microseconds(micros));
	}

private:
	LockTable locks;  // Thread-safe.
	Limiter limiter;  // Thread-safe.
	Limiter fdlimiter;  // Thread-safe.
};
#endif






