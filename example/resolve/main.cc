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

#ifdef _WIN32
#define format_64 "[%s: %I64d] "
#else
#define format_64 "[%s: %lld] "
#endif

namespace fs = std::experimental::filesystem;

boost::interprocess::file_lock fileLock;

bool exists(const std::string &fname)
{
	return fs::exists(fname.c_str());
}

bool lockFile(const std::string &fname)
{
	boost::interprocess::file_lock fl(fname.c_str());
	fileLock = std::move(fl);
	try
	{
		fileLock.lock();
		return true;
	}
	catch (boost::interprocess::interprocess_exception& e)
	{
		return false;
	}
}

bool unlockFile()
{
	try
	{
		fileLock.unlock();
		return true;
	}
	catch  (const std::exception &e) 
	{
		return false;
	}
}

int main()
{
	mysqlx_session_t  *sess;
	mysqlx_stmt_t     *crud;
	mysqlx_result_t   *res;
	mysqlx_row_t      *row;
	mysqlx_schema_t   *db;
	mysqlx_table_t    *table;

	char conn_error[MYSQLX_MAX_ERROR_LEN];
	int conn_err_code;

	sess = mysqlx_get_session("127.0.0.1",33060,"root","123456","game",conn_error, &conn_err_code);
	if (!sess)
	{
		printf("Error! %s. Error Code: %d\n", conn_error, conn_err_code);
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
			return 0;
		}

		std::string alter = "alter table log_item AUTO_INCREMENT = 1";
		res = mysqlx_sql(sess,alter.c_str(),MYSQLX_NULL_TERMINATED);
		RESULT_CHECK(res, sess);
	}

	const char *path = "log";
	std::string fileName;
	std::string feName;

	if (!fs::is_directory(path))
	{
		printf("path no exists\n");
	}
	else
	{
		const int MAX_LINE = 65536;
		while (1)
		{
			for (auto &it : fs::directory_iterator(path))
			{
				fileName += "log/";
				auto fe = it.path();
				feName = fe.filename().c_str();
				fileName += feName;
				
				if (!lockFile(fileName))
				{
					printf("lock..........\n");
					continue;
				}
				
				FILE *fp = ::fopen(fileName.c_str(),"r");
				assert(fp);
				fseek(fp,0,0);
				char buf[MAX_LINE];
				while (fgets(buf,MAX_LINE,fp) != nullptr)
				{
					char *start;
					start = strchr(buf,'#');
					if (start != nullptr)
					{
						printf("%s\n",start);
						start = start + 3;
						char *end;
						end = strchr(start,'#');
						assert(end != nullptr);
						printf("length %d\n",int(end - start));
						std::string ret(start,end);
						printf("string %s\n",ret.c_str());
						res = mysqlx_sql(sess,ret.c_str(),MYSQLX_NULL_TERMINATED);
						RESULT_CHECK(res, sess);
					}
				}

				feName.clear();
				fileName.clear();
				unlockFile();
				::fclose(fp);
				fs::remove(it.path());
			}
		}
	}
	return 0;
}
