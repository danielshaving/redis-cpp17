#include <stdio.h>
#include "status.h"


const char *Status::copyState(const char *state)
{
	uint32_t size;
	memcpy(&size,state,sizeof(size));
	char *result = (char*)zmalloc(size + 5);
	memcpy(result,state,size + 5);
	return result;
}

Status::Status(Code code,const std::string_view &msg,const std::string_view &msg2)
{
	assert(code != kOk);
	const uint32_t len1 = msg.size();
	const uint32_t len2 = msg2.size();
	const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
	char *result = (char*)zmalloc(size + 5);
	memcpy(result,&size,sizeof(size));
	result[4] = static_cast<char>(code);
	memcpy(result + 5,msg.data(),len1);
	if (len2)
	{
		result[5 + len1] = ':';
		result[6 + len1] = ' ';
		memcpy(result + 7 + len1,msg2.data(),len2);
	}
	state = result;
}

std::string Status::toString() const
{
	if (state == nullptr)
	{
		return "OK";
	} 
	else 
	{
		char tmp[30];
		const char *type;
		switch (code()) 
		{
			case kOk:
			type = "OK";
				break;
			case kNotFound:
			type = "NotFound: ";
				break;
			case kCorruption:
			type = "Corruption: ";
				break;
			case kNotSupported:
			type = "Not implemented: ";
				break;
			case kInvalidArgument:
			type = "Invalid argument: ";
				break;
			case kIOError:
			type = "IO error: ";
				break;
			default:
			snprintf(tmp,sizeof(tmp),"Unknown code(%d): ",static_cast<int>(code()));
			type = tmp;
			break;
		}

		std::string result(type);
		uint32_t length;
		memcpy(&length,state,sizeof(length));
		result.append(state + 5,length);
		return result;
	}
}
