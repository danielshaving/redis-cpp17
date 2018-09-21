#include <iostream>
#include <fstream>
#include <cstdlib>
#include <experimental/filesystem>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <mysqlx/xapi.h>
#include <chrono>
#include <thread>
#include <fstream>
#include <functional>
#include <algorithm>
#include <memory>
#include <sstream>
#include <fstream>

#include <boost/interprocess/sync/file_lock.hpp>

/* Error processing macros */
#define CRUD_CHECK(C, S) if (!C) \
  { \
    printf("Error! %s\n", mysqlx_error_message(S)); \
    return -1; \
  }

#define RESULT_CHECK(R, C) if (!R) \
  { \
    printf("Error! %s\n", mysqlx_error_message(C)); \
    return -1; \
  }

#define IS_OK(R, C)  if (R != RESULT_OK) \
  { \
    printf("Error! %s\n", mysqlx_error_message(C)); \
    return -1; \
  }

#ifdef _WIN64
#define format_64 "[%s: %I64d] "
#else
#define format_64 "[%s: %lld] "
#endif

namespace fs = std::experimental::filesystem;

boost::interprocess::file_lock fileLock;

void lockFile(const std::string &fname)
{
	try
	{
		boost::interprocess::file_lock fl(fname.c_str());
		fileLock = std::move(fl);
		fileLock.lock();
	}
	catch (boost::interprocess::interprocess_exception &e)
	{
		std::cout << e.what() << std::endl;
	}
}

void unlockFile()
{
	try
	{
		fileLock.unlock();
	}
	catch (boost::interprocess::interprocess_exception &e) 
	{
		std::cout << e.what() << std::endl;
	}
}

int main()
{
	mysqlx_session_t  *sess;
	mysqlx_result_t   *res;
	mysqlx_row_t      *row;

	char conn_error[MYSQLX_MAX_ERROR_LEN];
	int conn_err_code;

	sess = mysqlx_get_session("10.128.2.117", 33060, "root", "123456", "game", conn_error, &conn_err_code);
	if (!sess)
	{
		printf("Error! %s. Error Code: %d\n", conn_error, conn_err_code);
		system("pause");
		return -1;
	}

	printf("Connected...\n");

	{
		/*
		TODO: Only working with server version 8
		*/
		res = mysqlx_sql(sess,
			"show variables like 'version'",
			MYSQLX_NULL_TERMINATED);

		row = mysqlx_row_fetch_one(res);
		size_t len = 1024;
		char buffer[1024];

		if (RESULT_OK != mysqlx_get_bytes(row, 1, 0, buffer, &len))
			return -1;

		int major_version;
		major_version = atoi(buffer);
		mysqlx_result_free(res);

		if (major_version < 8)
		{
			printf("\nSession closed");
			mysqlx_session_close(sess);
			system("pause");
			return 0;
		}

		std::string alter = "alter table log_item AUTO_INCREMENT = 1";
		res = mysqlx_sql(sess, alter.c_str(), MYSQLX_NULL_TERMINATED);
		RESULT_CHECK(res, sess);
	}

	const char *path = "asynclog/";
	std::string fileName;
	std::string feName;

	if (!fs::is_directory(path))
	{
		printf("path no exists\n");
		system("pause");
	}
	else
	{
		//fs::directory_iterator end_iter;
		//for (fs::directory_iterator iter(path); iter != end_iter; ++iter)  
		for (auto&iter : fs::directory_iterator(path))
		{
			auto fe = iter.path();
				
			fileName.clear();
			fileName += path;
			fileName += fe.filename().string();

			printf("%s\n",fileName.c_str());
			if (!fs::exists(fileName))
			{
				printf("file no exists %s\n", fileName.c_str());
				assert(false);
			}
			//lockFile(fileName);
		
			std::ifstream fin(fileName.c_str()/*, ios::binary*/);
			if (fin.eof())
			{
				std::cout << "file is empty."<< std::endl;
				continue;
			}
		
			int i = 0;
			std::string line;
			while(std::getline(fin,line,'\r'))
			{
				//printf("%s\n",line.c_str());
				char *start = strchr(line.data(), '#');
				if (start != nullptr)
				{
					//printf("%s\n", start);
					start = start + 3;
					char *end;
					end = strchr(start, '#');
					if (end == nullptr)
					{
						//printf("%s\n",start);
						continue;
					}

					//printf("length %d\n", int(end - start));
					std::string ret(start, end);
					//printf("%s\n", ret.c_str());
					res = mysqlx_sql(sess, ret.c_str(), MYSQLX_NULL_TERMINATED);
					RESULT_CHECK(res, sess);
				}
			}
			
			fin.close();
			unlockFile();
			fs::remove(fe);
		}
	}
	return 0;
}
