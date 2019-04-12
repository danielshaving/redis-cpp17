#pragma once

#ifdef _WIN64
#include <windows.h>
#else 
#include <sys/time.h>
#endif
#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <thread>

class Logger {
public:
	// Creates a logger that writes to the given file.
	//
	// The PosixLogger instance takes ownership of the file handle.
	Logger(std::FILE *fp) : fp(fp) {
		assert(fp != nullptr);
	}

	void Log(const char *format, ...) {
		va_list ap;
		va_start(ap, format);
		logv(format, ap);
		va_end(ap);
	}

	void logv(const char *format, va_list arguments) {
		// Record the time as close to the Logv() call as possible.
#ifdef _WIN64
		SYSTEMTIME nowComponents;
		::GetLocalTime(&nowComponents);
#else
		struct ::timeval nowTimeval;
		::gettimeofday(&nowTimeval, nullptr);
		const std::time_t nowSeconds = nowTimeval.tv_sec;
		struct std::tm nowComponents;
		::localtime_r(&nowSeconds, &nowComponents);
#endif

		// Record the thread ID.
		constexpr const int kMaxThreadIdSize = 32;
		std::ostringstream thread_stream;
		thread_stream << std::this_thread::get_id();
		std::string thread_id = thread_stream.str();
		if (thread_id.size() > kMaxThreadIdSize) {
			thread_id.resize(kMaxThreadIdSize);
		}

		// We first attempt to print into a stack-allocated buffer. If this attempt
		// fails, we make a second attempt with a dynamically allocated buffer.
		constexpr const int kStackBufferSize = 512;
		char stackBuffer[kStackBufferSize];
		static_assert(sizeof(stackBuffer) == static_cast<size_t>(kStackBufferSize),
			"sizeof(char) is expected to be 1 in C++");

		int dynamicBufferSize = 0;  // Computed in the first iteration.
		for (int iteration = 0; iteration < 2; ++iteration) {
			const int bufferSize =
				(iteration == 0) ? kStackBufferSize : dynamicBufferSize;
			char *const buffer =
				(iteration == 0) ? stackBuffer : new char[dynamicBufferSize];

			// Print the header into the buffer.
#ifdef _WIN64
			int bufferOffset = snprintf(
				buffer, bufferSize,
				"%04d/%02d/%02d-%02d:%02d:%02d.%06d %s ",
				nowComponents.wYear,
				nowComponents.wMonth,
				nowComponents.wDay,
				nowComponents.wHour,
				nowComponents.wMinute,
				nowComponents.wSecond,
				static_cast<int>(nowComponents.wMilliseconds * 1000),
				thread_id.c_str());
#else
			int bufferOffset = snprintf(
				buffer, bufferSize,
				"%04d/%02d/%02d-%02d:%02d:%02d.%06d %s ",
				nowComponents.tm_year + 1900,
				nowComponents.tm_mon + 1,
				nowComponents.tm_mday,
				nowComponents.tm_hour,
				nowComponents.tm_min,
				nowComponents.tm_sec,
				static_cast<int>(nowTimeval.tv_usec),
				thread_id.c_str());
#endif

			// The header can be at most 28 characters (10 date + 15 time +
			// 3 delimiters) plus the thread ID, which should fit comfortably into the
			// static buffer.
			assert(bufferOffset <= 28 + kMaxThreadIdSize);
			static_assert(28 + kMaxThreadIdSize < kStackBufferSize,
				"stack-allocated buffer may not fit the message header");
			assert(bufferOffset < bufferSize);

			// Print the message into the buffer.
			std::va_list argumentsCopy;
			va_copy(argumentsCopy, arguments);
			bufferOffset += std::vsnprintf(buffer + bufferOffset,
				bufferSize - bufferOffset, format,
				argumentsCopy);
			va_end(argumentsCopy);

			// The code below may append a newline at the end of the buffer, which
			// requires an extra character.
			if (bufferOffset >= bufferSize - 1) {
				// The message did not fit into the buffer.
				if (iteration == 0) {
					// Re-run the loop and use a dynamically-allocated buffer. The buffer
					// will be large enough for the log message, an extra newline and a
					// null terminator.
					dynamicBufferSize = bufferOffset + 2;
					continue;
				}

				// The dynamically-allocated buffer was incorrectly sized. This should
				// not happen, assuming a correct implementation of (v)snprintf. Fail
				// in tests, recover by truncating the log message in production.
				assert(false);
				bufferOffset = bufferSize - 1;
			}

			// Add a newline if necessary.
			if (buffer[bufferOffset - 1] != '\n') {
				buffer[bufferOffset] = '\n';
				++bufferOffset;
			}

			assert(bufferOffset <= bufferSize);
			std::fwrite(buffer, 1, bufferOffset, fp);
			std::fflush(fp);

			if (iteration != 0) {
				delete[] buffer;
			}
			break;
		}
	}

	~Logger() {
		std::fclose(fp);
	}
private:
	std::FILE *const fp;
};