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

	const char *path = "../../../runtime/bin/asynclog/";
	std::string fileName;
	std::string feName;

	if (!fs::is_directory(path))
	{
		printf("path no exists\n");
		system("pause");
	}
	else
	{
		const int MAX_LINE = 65536;
		while (1)
		{
			std::this_thread::sleep_for(std::chrono::seconds(1));
			for (auto &it : fs::directory_iterator(path))
			{
				fileName += path;
				auto fe = it.path();
				fileName += fe.filename().string();

				if (!fs::exists(fileName))
				{
					printf("file no exists %s\n", fileName.c_str());
					break;
				}

				lockFile(fileName);

				FILE *fp = ::fopen(fileName.c_str(), "r");
				assert(fp);
				fseek(fp, 0, 0);
				char buf[MAX_LINE];
				while (fgets(buf, MAX_LINE, fp) != nullptr)
				{
					char *start;
					start = strchr(buf, '#');
					if (start != nullptr)
					{
						printf("%s\n", start);
						start = start + 3;
						char *end;
						end = strchr(start, '#');
						assert(end != nullptr);
						printf("length %d\n", int(end - start));
						std::string ret(start, end);
						printf("string %s\n", ret.c_str());
						res = mysqlx_sql(sess, ret.c_str(), MYSQLX_NULL_TERMINATED);
						RESULT_CHECK(res, sess);
					}
				}

				fileName.clear();
				unlockFile();
				::fclose(fp);
				fs::remove(it.path());
			}
		}
	}
	return 0;
}
