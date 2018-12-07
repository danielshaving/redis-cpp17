#pragma once
#include "filename.h"
#include "dbimpl.h"
#include "logging.h"
#include "posix.h"
#include "versionset.h"

class RecoveryTest
{
public:
	RecoveryTest(const Options &options, const std::string &dbname)
	:env(new PosixEnv()),
	db(new DBImpl(options, dbname)),
	dbname(dbname)
	{
		db->destroyDB(dbname, options);
		open();
	}

	~RecoveryTest()
	{
		db->destroyDB(dbname, options);
	}

	void close()
	{
		db.reset();	
	}
	
	void open(Options *options = nullptr) 
	{
		close();
		assert(openWithStatus(options).ok());
		assert(1 == numLogs());
	}
  
	Status openWithStatus(Options *options = nullptr)
	{
		close();
		Options opts;
		if (options != nullptr) 
		{
			opts = *options;
		}
		else 
		{
			opts.reuseLogs = true;  // TODO(sanjay): test both ways
			opts.createIfMissing = true;
		}
		
		if (opts.env == nullptr) 
		{
			opts.env = env;
		}

		db = std::shared_ptr<DBImpl>(new DBImpl(opts, dbname));
		return db->open();
	}
	
	bool canAppend() 
	{
		std::shared_ptr<PosixWritableFile> tmp;
		Status s = env->newAppendableFile(currentFileName(dbname), tmp);
		if (s.isNotSupportedError()) 
		{
			return false;
		}
		else
		{
			return true;
		}
	}

	Status put(const std::string &k, const std::string &v) 
	{
		return db->put(WriteOptions(), k, v);
	}

	std::string get(const std::string& k) 
	{
		std::string result;
		Status s = db->get(ReadOptions(), k, &result);
		if (s.isNotFound())
		{
			result = "NOT_FOUND";
		} 
		else if (!s.ok())
		{
			result = s.toString();
		}
		return result;
	}

	 
	std::string manifestFileName()
	{
		std::string current;
		assert(readFileToString(env.get(), currentFileName(dbname), &current).ok());
		size_t len = current.size();
		if (len > 0 && current[len-1] == '\n')
		{
			current.resize(len - 1);
		}
		return dbname + "/" + current;
	}

	std::string logName(uint64_t number)
	{
		return logFileName(dbname, number);
	}

	std::vector<uint64_t> getFiles(FileType t) 
	{
		std::vector<std::string> filenames;
		assert(env->getChildren(dbname, &filenames).ok());
		std::vector<uint64_t> result;
		for (size_t i = 0; i < filenames.size(); i++)
		{
			uint64_t number;
			FileType type;
			if (parseFileName(filenames[i], &number, &type) && type == t) 
			{
				result.push_back(number);
			}
		}
		return result;
	}
	 
	size_t deleteLogFiles()
	{
		std::vector<uint64_t> logs = getFiles(kLogFile);
		for (size_t i = 0; i < logs.size(); i++) 
		{
			assert(env->deleteFile(logName(logs[i])).ok());
		}
		return logs.size();
	}

	void deleteManifestFile() 
	{
		assert(env->deleteFile(manifestFileName()).ok());
	}

	uint64_t firstLogFile()
	{
		return getFiles(kLogFile)[0];
	}

	int numLogs() 
	{
		return getFiles(kLogFile).size();
	}

	int numTables() 
	{
		return getFiles(kTableFile).size();
	}

	uint64_t fileSize(const std::string& fname) 
	{
		uint64_t result;
		assert(env->getFileSize(fname, &result).ok());
		return result;
	}
  
	void manifestReused()
	{
		if (!canAppend()) 
		{
			fprintf(stderr, "skipping test because env does not support appending\n");
			return;
		}

		assert(put("foo", "bar").ok());
		close();

		std::string oldManifest = manifestFileName();
		open();
		assert(oldManifest == manifestFileName());
		assert("bar" == get("foo"));
		open();

		assert(oldManifest == manifestFileName());
		assert("bar" == get("foo"));
	}
	
private:
	const Options options;
	const std::string dbname;
	std::shared_ptr<PosixEnv> env;
	std::shared_ptr<DBImpl> db;
};


int main()
{
	Options options;
	const std::string dbname = "recovery";
	RecoveryTest rtest(options, dbname);
	rtest.manifestReused();
	return 0;
}
