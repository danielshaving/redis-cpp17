#include "dbimpl.h"
#include "option.h"
#include "cache.h"
#include "coding.h"
#include <vector>
#include <functional>

// Conversions between numeric keys/values and the types expected by Cache.
static std::string encodeKey(int k) 
{
	std::string result;
	putFixed32(&result, k);
	return result;
}

static int decodeKey(const std::string_view &k) 
{
	assert(k.size() == 4);
	return decodeFixed32(k.data());
}

static int decodeValue(const std::any &v) 
{
	assert(v.has_value());
	return std::any_cast<int>(v);
}

class CacheTest 
{
public:
	void deleterCallBack(const std::string_view &key, const std::any &v)
	{
		deletedKeys.push_back(decodeKey(key));
		deletedValues.push_back(decodeValue(v));
	}
	
	CacheTest()
	:cache(new ShardedLRUCache(kCacheSize)) 
	{
		
	}
  
	int lookup(int key) 
	{
		auto handle = cache->lookup(encodeKey(key));
		const int r = (handle == nullptr) ? -1 : decodeValue(cache->value(handle));
		if (handle != nullptr) 
		{
			cache->erase(encodeKey(key));
		}
		return r;
	}

	void insert(int key, int value, int charge = 1) 
	{
		cache->insert(encodeKey(key), value, charge,
			std::bind(&CacheTest::deleterCallBack, this,
			std::placeholders::_1, std::placeholders::_2));
	}

	void erase(int key) 
	{
		cache->erase(encodeKey(key));
	}
	
	void hitAndMiss()
	{
		assert(lookup(100) == -1);
		insert(100, 101);
		assert(lookup(100) == 101);
		assert(lookup(200) == -1);
		assert(lookup(300) == -1);
		
		assert(deletedKeys.size() == 1);
		assert(deletedKeys[0] == 100);
		assert(deletedValues[0] == 101);
	}
		
private:
	static const int kCacheSize = 1000;
	std::vector<int> deletedKeys;
	std::vector<int> deletedValues;
	std::shared_ptr<ShardedLRUCache> cache;
};
	
int main(int argc, char *argv[])
{
	CacheTest test;
	test.hitAndMiss();
	
	/*Options options;
	options.createIfMissing = true;

	DBImpl db(options, "db");
	Status s = db.open();
	if (!s.ok())
	{
		printf("%s\n", s.toString().c_str());
	}

	for (int i = 0; i < 100; i++)
	{
		std::string key = std::to_string(i);
		std::string value = std::to_string(i);
		//std::string v;
		s = db.put(WriteOptions(), key, value);
		assert(s.ok());
		//s = db.get(ReadOptions(),key,&v);
		//assert(s.ok());
	}

	for (int i = 101; i < 1010; i++)
	{
		std::string key = std::to_string(i);
		std::string v;
		s = db.get(ReadOptions(), key, &v);
		assert(s.ok());
	}
	*/
	return 0;
	
}
