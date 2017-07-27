#pragma once

#include "all.h"
#include "xLog.h"

typedef std::map<std::string, std::string> INI_DATA_MAP;
class xINIParser
 {
	private:
		INI_DATA_MAP map;

	public:
		xINIParser(const char *file, const char *title)
		{
			getDataFromFile(file, title);
		}

		~xINIParser() { map.clear(); }

		std::string getStringData(const char* key)
		{
			auto iter = map.find(key);
			if (iter != map.end())
			{
				return iter->second;
			}
			std::string str;
			return str;
		}

		int getNumberData(const char* key)
		{
			auto iter = map.find(key);
			if (iter != map.end())
			{
				return atoi(iter->second.c_str());
			}
			return 0;
		}

		INI_DATA_MAP getMap() { return map; }

	private:
		void getDataFromFile(const char* file, const char *title)  
		{  
			char *value, sect[30], c;  
			char linebuf[1024];  
			FILE *inifp;
			strcpy(sect, "[");  
			strcat(sect, title);  
			strcat(sect, "]"); 

			if ((inifp = fopen(file, "rb")) == nullptr)
			{
				LOG_WARN<<"read file error";
				return;
			}


			while ((c = fgetc(inifp)) != EOF)  
			{  
				if (c == '[' && c != '#')  
				{  
					ungetc(c, inifp);  
					fgets(linebuf, 1024, inifp);  
					if (strstr(linebuf, sect))  
					{  
						while ((c = fgetc(inifp)) != '[' && c != EOF)  
						{  
							ungetc(c, inifp);  
							fgets(linebuf, 1024, inifp);

							if (linebuf[0] == '#') continue;

							char strKey[64] = "", strValue[512] = "";

						    char *ptr;  
						    char *p; 
						    ptr = strtok_r(linebuf, "=\n\r ", &p);  
						    bool bValid = false;
						    if (ptr != nullptr)
						    {
						    	strcpy(strKey, ptr); 
						        ptr = strtok_r(nullptr, "=\n\r ", &p);
						        if (ptr != nullptr)
						        {
						        	strcpy(strValue, ptr);
						        	bValid = true; 
						        }
						    }
						    if (bValid) map.insert(INI_DATA_MAP::value_type(strKey, strValue));
						}  
						if (c==EOF) continue;

						ungetc(c, inifp);  
					}
				}  
				else  
				{  
					ungetc(c, inifp);  
					fgets(linebuf, 1024, inifp);  
				}  
			} 

			fclose(inifp); 
		} 
};
