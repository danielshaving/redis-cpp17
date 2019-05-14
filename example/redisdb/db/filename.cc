#include <ctype.h>
#include <stdio.h>
#include "filename.h"
#include "logging.h"
#include "env.h"


static std::string makeFileName(const std::string& dbname, uint64_t number,
	const char* suffix) {
	char buf[100];
	snprintf(buf, sizeof(buf), "/%06llu.%s", static_cast<unsigned long long>(number), suffix);
	return dbname + buf;
}

std::string LogFileName(const std::string& dbname, uint64_t number) {
	assert(number > 0);
	return makeFileName(dbname, number, "log");
}

std::string TableFileName(const std::string& dbname, uint64_t number) {
	assert(number > 0);
	return makeFileName(dbname, number, "ldb");
}

std::string SSTableFileName(const std::string& dbname, uint64_t number) {
	assert(number > 0);
	return makeFileName(dbname, number, "sst");
}

std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
	assert(number > 0);
	char buf[100];
	snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
		static_cast<unsigned long long>(number));
	return dbname + buf;
}

std::string CurrentFileName(const std::string& dbname) {
	return dbname + "/CURRENT";
}

std::string LockFileName(const std::string& dbname) {
	return dbname + "/LOCK";
}

std::string TempFileName(const std::string& dbname, uint64_t number) {
	assert(number > 0);
	return makeFileName(dbname, number, "dbtmp");
}

std::string InfoLogFileName(const std::string& dbname) {
	return dbname + "/LOG";
}

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname) {
	return dbname + "/LOG.old";
}


// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool ParseFileName(const std::string& filename, uint64_t* number, FileType* type) {
	std::string_view rest(filename);
	if (rest == "CURRENT") {
		*number = 0;
		*type = kCurrentFile;
	}
	else if (rest == "LOCK") {
		*number = 0;
		*type = kDBLockFile;
	}
	else if (rest == "LOG" || rest == "LOG.old") {
		*number = 0;
		*type = kInfoLogFile;
	}
	else if (StartsWith(rest, "MANIFEST-")) {
		rest.remove_prefix(strlen("MANIFEST-"));
		uint64_t num;
		if (!ConsumeDecimalNumber(&rest, &num)) {
			return false;
		}

		if (!rest.empty()) {
			return false;
		}

		*type = kDescriptorFile;
		*number = num;
	}
	else {
		// Avoid strtoull() to keep filename format independent of the
		// current locale
		uint64_t num;
		if (!ConsumeDecimalNumber(&rest, &num)) {
			return false;
		}
		std::string_view suffix = rest;
		if (suffix == std::string_view(".log")) {
			*type = kLogFile;
		}
		else if (suffix == std::string_view(".sst") || suffix == std::string_view(".ldb")) {
			*type = kTableFile;
		}
		else if (suffix == std::string_view(".dbtmp")) {
			*type = kTempFile;
		}
		else {
			return false;
		}
		*number = num;
	}
	return true;
}

Status SetCurrentFile(const std::shared_ptr<Env>& env, const std::string& dbname, uint64_t descriptorNumber) {
	std::string manifest = DescriptorFileName(dbname, descriptorNumber);
	std::string_view Contents = manifest;
	assert(StartsWith(Contents, dbname + "/"));

	Contents.remove_prefix(dbname.size() + 1);
	std::string tmp = TempFileName(dbname, descriptorNumber);
	Status s = WriteStringToFileSync(env, std::string(Contents.data(), Contents.size()) + "\n", tmp);
	if (s.ok()) {
		s = env->RenameFile(tmp, CurrentFileName(dbname));
	}

	if (!s.ok()) {
		env->DeleteFile(tmp);
	}
	return s;
}
