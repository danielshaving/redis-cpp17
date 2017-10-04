#pragma once
#include "all.h"

class xHttpRequest
{
public:

xHttpRequest()
    : method(kInvalid),
      version(kUnknown)
  {
  }

 enum Method
{
	 kInvalid, kGet, kPost, kHead, kPut, kDelete,kContent
};
enum Version
{
	kUnknown, kHttp10, kHttp11
};

void setVersion(Version v)
{
	version = v;
}
Version getVersion()const
{
	return version;
}

void setMethod()
{
	method = kContent;
}

bool setMethod(const char * start,const char * end)
{
	assert(method == kInvalid);
	std::string m(start,end);
	if (m == "GET")
	{
	  method = kGet;
	}
	else if (m == "POST")
	{
	  method = kPost;
	}
	else if (m == "HEAD")
	{
	  method = kHead;
	}
	else if (m == "PUT")
	{
	  method = kPut;
	}
	else if (m == "DELETE")
	{
	  method = kDelete;
	}
	else
	{
	  method = kInvalid;
	}
	

	return method != kInvalid;

}

Method getMethod()const
{
	return method;
}

const char * methodString()const
{
	const char * result = "UNKNOWN";
	switch(method)
	{
		case kGet:
		{
			result = "GET";
			break;
		}
		case kPost:
		{
			result = "POST";
			break;
		}
		case kHead:
		{
			result = "HEAD";
			break;
		}
		case kPut:
		{
			result = "PUT";
			break;
		}
		case kDelete:
		{
			result = "DELETE";
			break;
		}
		default:
			break;
			
	}
	return result;
}

std::string &getPath()
{
	return path;
}

void setPath(const char *start,const char *end)
{
	path.assign(start,end);
}

void setQuery(const char *start,const char * end)
{
	query.assign(start,end);
}

const std::string &getQuery()const
{
	return query;
}

void setReceiveTime(int64_t t)
{
	receiveTime = t;
}

void addContent(const char *start,const char *colon,const char *end)
{
	std::string field(start,colon);
	++colon;
	while(colon < end && isspace(*colon))
	{
		++colon;
	}
	std::string value(colon,end);
	while (!value.empty() && isspace(value[value.size()-1]))
	{
		value.resize(value.size()-1);
	}
	contentLength = atoi(value.c_str());
}
void addHeader(const char *start,const char *colon,const char *end)
{
	std::string field(start,colon);
	++colon;
	while(colon < end && isspace(*colon))
	{
		++colon;
	}
	std::string value(colon,end);
	while (!value.empty() && isspace(value[value.size()-1]))
	{
		value.resize(value.size()-1);
	}
	headers[field] = value;
}

std::string getHeader(const std::string& field) const
{
	std::string result;
	std::map<std::string, std::string>::const_iterator it = headers.find(field);
	if (it != headers.end())
	{
	  result = it->second;
	}
	return result;
}

const std::map<std::string, std::string>& getHeaders() const
{
	return headers;
}

void swap(xHttpRequest& that)
{
	std::swap(method, that.method);
	path.swap(that.path);
	query.swap(that.query);
	headers.swap(that.headers);
}
  
private:
	Method method;
	Version version;
	std::string path;
	std::string query;
	int32_t queryLength = -1;
	std::map<std::string,std::string> headers;
	int64_t receiveTime = -1;
	int32_t contentLength = -1;
};
