#pragma once

#include <stdint.h>
#include <string>
#include <string.h>
#include <assert.h>
#include "status.h"

class PosixEnv;

enum FileType
{
	kLogFile,
	kDBLockFile,
	kTableFile,
	kDescriptorFile,
	kCurrentFile,
	kTempFile,
	kInfoLogFile  // Either the current one, or an old one
};

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
std::string logFileName(const std::string &dbname, uint64_t number);

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
std::string tableFileName(const std::string &dbname, uint64_t number);

// Return the legacy file name for an sstable with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
std::string sstTableFileName(const std::string &dbname, uint64_t number);

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
std::string descriptorFileName(const std::string &dbname, uint64_t number);

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
std::string currentFileName(const std::string &dbname);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
std::string lockFileName(const std::string &dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
std::string tempFileName(const std::string &dbname, uint64_t number);

// Return the name of the info log file for "dbname".
std::string infoLogFileName(const std::string &dbname);

// Return the name of the old info log file for "dbname".
std::string oldInfoLogFileName(const std::string &dbname);

// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
bool parseFileName(const std::string &filename, uint64_t *number, FileType* type);

// Make the CURRENT file point to the descriptor file with the
// specified number.
Status setCurrentFile(PosixEnv *env, const std::string &dbname, uint64_t descriptorNumber);

