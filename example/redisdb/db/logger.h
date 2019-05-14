#pragma once

#include <stdint.h>
#include <cstdarg>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <algorithm>
#include <stdio.h>
#include <time.h>
#include <fcntl.h>
#include "env.h"

class PosixLogger : public Logger {
private:
	Status posixCloseHelper() {
		int ret;

		ret = ::fclose(file);
		if (ret) {
			return PosixError("Unable to close log file", ret);
		}
		return Status::OK();
	}

	FILE* file;
	std::atomic_size_t logsize;
	int fd;
	const static uint64_t flushevertyseconds = 5;
	std::atomic_uint_fast64_t lastflushmicros;
	Env env;
	std::atomic<bool> flushpending;

protected:
	virtual Status CloseImpl() { return posixCloseHelper(); }

public:
	PosixLogger(FILE* f, const InfoLogLevel level = InfoLogLevel::ERROR_LEVEL)
		: Logger(level),
		file(f),
		logsize(0),
		fd(fileno(f)),
		lastflushmicros(0),
		flushpending(false) {
	}

	virtual ~PosixLogger() {
		if (!closed) {
			closed = true;
			posixCloseHelper();
		}
	}
	virtual void flush() {
		if (flushpending) {
			flushpending = false;
			fflush(file);
		}
		lastflushmicros = env.NowMicros();
	}

	using Logger::Logv;
	virtual void Logv(const char* format, va_list ap) {
		const uint64_t threadid = ::getpid();

		// We try twice: the first time with a fixed-size stack allocated buffer,
		// and the second time with a much larger dynamically allocated buffer.
		char buffer[500];
		for (int iter = 0; iter < 2; iter++) {
			char* base;
			int bufsize;
			if (iter == 0) {
				bufsize = sizeof(buffer);
				base = buffer;
			}
			else {
				bufsize = 65536;
				base = new char[bufsize];
			}

			char* p = base;
			char* limit = base + bufsize;

			struct timeval nowtv;
			gettimeofday(&nowtv, nullptr);
			const time_t seconds = nowtv.tv_sec;
			struct tm t;
			localtime_r(&seconds, &t);
			p += snprintf(p, limit - p,
				"%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
				t.tm_year + 1900,
				t.tm_mon + 1,
				t.tm_mday,
				t.tm_hour,
				t.tm_min,
				t.tm_sec,
				static_cast<int>(nowtv.tv_usec),
				static_cast<long long unsigned int>(threadid));

			// Print the message
			if (p < limit) {
				va_list backupap;
				va_copy(backupap, ap);
				p += vsnprintf(p, limit - p, format, backupap);
				va_end(backupap);
			}

			// Truncate to available space if necessary
			if (p >= limit) {
				if (iter == 0) {
					continue;       // Try again with larger buffer
				}
				else {
					p = limit - 1;
				}
			}

			// Add newline if necessary
			if (p == base || p[-1] != '\n') {
				*p++ = '\n';
			}

			assert(p <= limit);
			const size_t writesize = p - base;

#ifdef _FALLOCATE_PRESENT
			const int kDebugLogChunkSize = 128 * 1024;

			// If this Write would cross a boundary of kDebugLogChunkSize
			// space, pre-allocate more space to avoid overly large
			// allocations from filesystem allocsize options.
			const size_t log_size = logsize;
			const size_t last_allocation_chunk =
				((kDebugLogChunkSize - 1 + log_size) / kDebugLogChunkSize);
			const size_t desired_allocation_chunk =
				((kDebugLogChunkSize - 1 + log_size + writesize) /
					kDebugLogChunkSize);
			if (last_allocation_chunk != desired_allocation_chunk) {
				fallocate(
					fd, FALLOC_FL_KEEP_SIZE, 0,
					static_cast<off_t>(desired_allocation_chunk * kDebugLogChunkSize));
			}
#endif

			size_t sz = ::fwrite(base, 1, writesize, file);
			flushpending = true;
			if (sz > 0) {
				logsize += writesize;
			}

			uint64_t nowmicros = static_cast<uint64_t>(nowtv.tv_sec) * 1000000 +
				nowtv.tv_usec;
			if (nowmicros - lastflushmicros >= flushevertyseconds * 1000000) {
				flush();
			}

			if (base != buffer) {
				delete[] base;
			}
			break;
		}
	}
	size_t GetLogFileSize() const { return logsize; }
};
