#pragma once

class StringArg
{
public:
	StringArg(const char *str)
	:str(str)
	{

	}
	StringArg(const std::string &str)
	:str(str.c_str())
	{

	}
	const char *c_str() const { return str; }
private:
	const char *str;
};

class StringPiece
{
 public:
	StringPiece()
	: ptr(nullptr), length(0) { }

	StringPiece(const char *str)
	: ptr(str),length(static_cast<int32_t>(strlen(ptr))) { }

	StringPiece(const unsigned char *str)
	: ptr(reinterpret_cast<const char *>(str)),
	length(static_cast<int32_t>(strlen(ptr))) { }

	StringPiece(const std::string &str)
	: ptr(str.data()),length(static_cast<int32_t>(str.size())) { }

	StringPiece(const char* offset,int32_t length)
	: ptr(offset),length(length) { }

	std::string as_string() const { return std::string(data(),size());}
	const char *ptr;
	int32_t length;

	bool empty() const { return length == 0; }
	const char *begin() const  { return ptr; }
	const char *end() const { return ptr + length; }
	int32_t size() const { return length; }
	const char *data() const { return ptr; }

	void removePrefix(int32_t n)
	{
		ptr += n;
		length -= n;
	}

	void removeSuffix(int32_t n)
	{
		length -= n;
	}

	void clear() { ptr = nullptr; length = 0; }
	void set(const char *buffer,int32_t len) { ptr = buffer; length = len; }
	void set(const char *str)
	{
		ptr = str;
		length = static_cast<int32_t>(strlen(str));
	}

	void set(const void *buffer,int32_t len)
	{
		ptr = reinterpret_cast<const char*>(buffer);
		length = len;
	}

  	char operator[](int32_t i) const { return ptr[i]; }

	bool operator==(const StringPiece &x) const
	{
		return ((length == x.length) && (memcmp(ptr, x.ptr,length) == 0));
	}
	
	bool operator!=(const StringPiece &x) const
	{
	  	return !(*this == x);
	}

	int compare(const StringPiece &x) const
	{
		int r = memcmp(ptr,x.ptr,length < x.length ? length : x.length);
		if (r == 0)
		{
			if (length < x.length) r = -1;
			else if (length > x.length) r = +1;
		}
		return r;
	}

	void copyToString(std::string *target) const
	{
		target->assign(ptr,length);
	}

	bool startsWith(const StringPiece &x) const
	{
		return ((length >= x.length) && (memcmp(ptr,x.ptr,x.length) == 0));
	}
};
