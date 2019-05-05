#include "env.h"
#include "logger.h"

Status Env::newLogger(const std::string& fname, std::shared_ptr<Logger>& result) {
	FILE* f = ::fopen(fname.c_str(), "w");
	if (f == nullptr) {
		result.reset();
		return PosixError("when fopen a file for new logger", errno);
	}
	else {
		result.reset(new PosixLogger(f));
		return Status::OK();
	}
}

Logger::~Logger() {}

Status Logger::close() {
	if (!closed) {
		closed = true;
		return closeImpl();
	}
	else {
		return Status::OK();
	}
}

Status Logger::closeImpl() { return Status::notSupported(""); }

void LogFlush(Logger* infolog) {
	if (infolog) {
		infolog->flush();
	}
}

static void Logv(Logger* infolog, const char* format, va_list ap) {
	if (infolog && infolog->getInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
		infolog->logv(InfoLogLevel::INFO_LEVEL, format, ap);
	}
}

void Log(Logger* infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Logv(infolog, format, ap);
	va_end(ap);
}

void Logger::logv(const InfoLogLevel level, const char* format, va_list ap) {
	static const char* kInfoLogLevelNames[5] = { "DEBUG", "INFO", "WARN",
	  "ERROR", "FATAL" };
	if (level < loglevel) {
		return;
	}

	if (level == InfoLogLevel::INFO_LEVEL) {
		// Doesn't print log level if it is INFO level.
		// This is to avoid unexpected performance regression after we add
		// the feature of log level. All the logs before we add the feature
		// are INFO level. We don't want to add extra costs to those existing
		// logging.
		logv(format, ap);
	}
	else if (level == InfoLogLevel::HEADER_LEVEL) {
		logHeader(format, ap);
	}
	else {
		char newformat[500];
		snprintf(newformat, sizeof(newformat) - 1, "[%s] %s",
			kInfoLogLevelNames[level], format);
		logv(newformat, ap);
	}
}

static void Logv(const InfoLogLevel level, Logger* infolog, const char* format, va_list ap) {
	if (infolog && infolog->getInfoLogLevel() <= level) {
		if (level == InfoLogLevel::HEADER_LEVEL) {
			infolog->logHeader(format, ap);
		}
		else {
			infolog->logv(level, format, ap);
		}
	}
}

void Log(const InfoLogLevel log_level, Logger* infolog, const char* format,
	...) {
	va_list ap;
	va_start(ap, format);
	Logv(log_level, infolog, format, ap);
	va_end(ap);
}

static void Headerv(Logger* infolog, const char* format, va_list ap) {
	if (infolog) {
		infolog->logHeader(format, ap);
	}
}

void Header(Logger* infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Headerv(infolog, format, ap);
	va_end(ap);
}

static void Debugv(Logger* infolog, const char* format, va_list ap) {
	if (infolog && infolog->getInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
		infolog->logv(InfoLogLevel::DEBUG_LEVEL, format, ap);
	}
}

void Debug(Logger* infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Debugv(infolog, format, ap);
	va_end(ap);
}

static void Infov(Logger* infolog, const char* format, va_list ap) {
	if (infolog && infolog->getInfoLogLevel() <= InfoLogLevel::INFO_LEVEL) {
		infolog->logv(InfoLogLevel::INFO_LEVEL, format, ap);
	}
}

void Info(Logger* infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Infov(infolog, format, ap);
	va_end(ap);
}

static void Warnv(Logger* infolog, const char* format, va_list ap) {
	if (infolog && infolog->getInfoLogLevel() <= InfoLogLevel::WARN_LEVEL) {
		infolog->logv(InfoLogLevel::WARN_LEVEL, format, ap);
	}
}

void Warn(Logger* infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Warnv(infolog, format, ap);
	va_end(ap);
}

static void Errorv(Logger* infolog, const char* format, va_list ap) {
	if (infolog && infolog->getInfoLogLevel() <= InfoLogLevel::ERROR_LEVEL) {
		infolog->logv(InfoLogLevel::ERROR_LEVEL, format, ap);
	}
}

void Error(Logger* infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Errorv(infolog, format, ap);
	va_end(ap);
}

static void Fatalv(Logger* infolog, const char* format, va_list ap) {
	if (infolog && infolog->getInfoLogLevel() <= InfoLogLevel::FATAL_LEVEL) {
		infolog->logv(InfoLogLevel::FATAL_LEVEL, format, ap);
	}
}

void Fatal(Logger* infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Fatalv(infolog, format, ap);
	va_end(ap);
}

void LogFlush(const std::shared_ptr<Logger>& infolog) {
	LogFlush(infolog.get());
}

void Log(const InfoLogLevel log_level, const std::shared_ptr<Logger>& infolog,
	const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Logv(log_level, infolog.get(), format, ap);
	va_end(ap);
}

void Header(const std::shared_ptr<Logger>& infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Headerv(infolog.get(), format, ap);
	va_end(ap);
}

void Debug(const std::shared_ptr<Logger>& infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Debugv(infolog.get(), format, ap);
	va_end(ap);
}

void Info(const std::shared_ptr<Logger>& infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Infov(infolog.get(), format, ap);
	va_end(ap);
}

void Warn(const std::shared_ptr<Logger>& infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Warnv(infolog.get(), format, ap);
	va_end(ap);
}

void Error(const std::shared_ptr<Logger>& infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Errorv(infolog.get(), format, ap);
	va_end(ap);
}

void Fatal(const std::shared_ptr<Logger>& infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Fatalv(infolog.get(), format, ap);
	va_end(ap);
}

void Log(const std::shared_ptr<Logger>& infolog, const char* format, ...) {
	va_list ap;
	va_start(ap, format);
	Logv(infolog.get(), format, ap);
	va_end(ap);
}