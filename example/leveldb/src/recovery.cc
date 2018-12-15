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
		std::shared_ptr<WritableFile> tmp;
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

	std::string get(const std::string &k) 
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

	void largeManifestCompacted()
	{
		if (!canAppend()) 
		{
			fprintf(stderr, "skipping test because env does not support appending\n");
			return;
		}

		open();
		assert(put("foo", "bar").ok());

		std::string oldManifest = manifestFileName();
		
		// Pad with zeroes to make manifest file very big.
		{
			uint64_t len = fileSize(oldManifest);
			std::shared_ptr<WritableFile> file;
			options.env->newAppendableFile(oldManifest, file);
			std::string zeroes(3*1048576 - static_cast<size_t>(len), 0);
			assert(file->append(zeroes).ok());
			assert(file->flush().ok());
		}

		open();
		std::string newManifest = manifestFileName();
		assert(oldManifest != newManifest);
		assert(10000 > fileSize(newManifest));
		assert("bar" == get("foo"));

		open();
		assert(newManifest == manifestFileName());
		assert("bar" == get("foo"));
	}

	void noLogFiles()
	{
		assert(put("foo", "bar").ok());
		assert(1 ==  deleteLogFiles());
		open();
		assert("NOT_FOUND" == get("foo"));
		open();
		assert("NOT_FOUND" == get("foo"));
	}

	void logFileReuse()
	{
		if (!canAppend()) 
		{
			fprintf(stderr, "skipping test because env does not support appending\n");
			return;
		}

		for (int i =0; i < 2; i++)
		{
			assert(put("foo", "bar").ok());
			if (i == 0)
			{
				 // Compact to ensure current log is empty
  				 compactMemTable();
			}

			close();
			assert(1 == numLogs());
			uint64_t number = firstLogFile();
			if (i == 0) 
			{
				assert(0 == fileSize(logName(number)));
			}
			else
			{
				assert(0 < fileSize(logName(number)));
			}
			
			open();
			assert(1 == numLogs());
			assert(number == firstLogFile());
			assert("bar" == get("foo"));
			open();
			assert(1 == numLogs());
			assert(number == firstLogFile());
			assert("bar" == get("foo"));
		}
	}

	void multipleMemTables()
	{
		const int kNum = 1000;
		for (int i = 0; i < kNum; i++) 
		{
			char buf[100];
			snprintf(buf, sizeof(buf), "%050d", i);
			assert(put(buf, buf).ok());
		}

		assert(0 == numTables());
		close();
		assert(0 == numTables());
		assert(1 == numLogs());
		uint64_t oldLogFile = firstLogFile();

		// Force creation of multiple memtables by reducing the write buffer size.
		Options opt;
		opt.reuseLogs = true;
		opt.writeBufferSize = (kNum*100) / 2;
		open(&opt);
		assert(2 <= numTables());
		assert(1 == numLogs());
		assert(oldLogFile != firstLogFile());
		for (int i = 0; i < kNum; i++) 
		{
			char buf[100];
			snprintf(buf, sizeof(buf), "%050d", i);
			assert(buf == get(buf));
		}
		
	}

	void singleTables()
	{
		const int kNum = 1000000;
		for (int i = 0; i < kNum; i++) 
		{
			char buf[100];
			snprintf(buf, sizeof(buf), "%050d", 100);
			assert(put(buf, buf).ok());
		}

		close();
		open();

		for (int i = 0; i < kNum; i++) 
		{
			char buf[100];
			snprintf(buf, sizeof(buf), "%050d", 100);
			assert(buf == get(buf));
		}
		
	}
	

	void compactMemTable()
	{
		 db->testCompactMemTable();
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
	/*rtest.manifestReused();
	rtest.largeManifestCompacted();
	rtest.noLogFiles();
	rtest.logFileReuse();
	*/
	rtest.multipleMemTables();
	return 0;
}
