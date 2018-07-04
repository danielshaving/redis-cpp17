/*
 * main.cpp
 *
 *  Created on: 2018 7 4
 *      Author: zhanghao
 */

#include <iostream>
#include <fstream>
#include <cstdlib>
#include <experimental/filesystem>
#include <stdio.h>
#include <assert.h>
#include <string.h>
namespace fs = std::experimental::filesystem;

int main()
{
	const char *path = "redislog";
	std::string fileName;
	std::string feName;

	if(!fs::is_directory(path))
	{
		printf("path no exists\n");
	}
	else
	{
		const int MAX_LINE = 65536;
		for (auto &it : fs::directory_iterator(path))
		{
			fileName += "redislog/";
			auto fe = it.path();
			feName = fe.filename().c_str();
			fileName += feName;
			FILE *fp = ::fopen(fileName.c_str(),"r");
			assert(fp);
			fseek(fp,0,0);
			char buf[MAX_LINE];
			while(fgets(buf,MAX_LINE,fp) != nullptr)
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
				    printf("string :%s\n",ret.c_str());
			    }
			}

			feName.clear();
			fileName.clear();
			::fclose(fp);
		}

	}
	return 0;
}
