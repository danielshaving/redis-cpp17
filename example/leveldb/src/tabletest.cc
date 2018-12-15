#pragma once
#include <map>
#include <string>
#include <assert.h>
#include <string_view>
#include <iostream>
#include <random>
#include "table.h"
#include "dbformat.h"
#include "memtable.h"
#include "dbimpl.h"
#include "posix.h"
#include "blockbuilder.h"
#include "tablebuilder.h"


// Return reverse of "key".
// Used to test non-lexicographic comparators.
std::string reverse(const std::string_view &key) 
{
	std::string str(key.data(), key.size());
	std::string rev("");
	for (std::string::reverse_iterator rit = str.rbegin();
	   rit != str.rend(); ++rit) 
	{
		rev.push_back(*rit);
	}
	return rev;
}

class ReverseKeyComparator : public Comparator 
{
public:
	ReverseKeyComparator()
	:comparator(new BytewiseComparatorImpl())
	{

	}

	virtual const char *name() const 
	{
		return "leveldb.ReverseBytewiseComparator";
	}

	virtual int compare(const std::string_view &a, const std::string_view &b) const 
	{
		return comparator->compare(reverse(a), reverse(b));
	}

	virtual void findShortestSeparator(
	  std::string *start,
	  const std::string_view &limit) const
	{
		std::string s = reverse(*start);
		std::string l = reverse(limit);
		comparator->findShortestSeparator(&s, l);
		*start = reverse(s);
	}

	virtual void findShortSuccessor(std::string *key) const 
	{
		std::string s = reverse(*key);
		comparator->findShortSuccessor(&s);
		*key = reverse(s);
	}
private:
	std::shared_ptr<Comparator> comparator;
};

std::shared_ptr<ReverseKeyComparator> reverseCmp = nullptr;

static void increment(const Comparator *cmp, std::string *key) 
{
	if (!reverseCmp) 
	{
		key->push_back('\0');
	} 
	else 
	{
		std::string rev = reverse(*key);
		rev.push_back('\0');
		*key = reverse(rev);
	}
}

// Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.

struct STLLessThan
{
	const Comparator *cmp;
	STLLessThan(const Comparator *c)
	:cmp(c)
	{

	}

	bool operator()(const std::string &a, const std::string &b) const
	{
		return cmp->compare(std::string_view(a), std::string_view(b)) < 0;
	}
};

class StringSink : public WritableFile
{
public:
	virtual ~StringSink() { }

	const std::string &getContents() const { return contents; }

	virtual Status close() { return Status::OK(); }
	virtual Status flush() { return Status::OK(); }
	virtual Status sync() { return Status::OK(); }

	virtual Status append(const std::string_view &data) 
	{
		contents.append(data.data(), data.size());
		return Status::OK();
	}

private:
	std::string contents;
};

class StringSource : public RandomAccessFile 
{
public:
	StringSource(const std::string_view &c)
	: contents(c.data(), c.size())
	{

	}

	virtual ~StringSource() { }

	uint64_t size() const { return contents.size(); }

	virtual Status read(uint64_t offset, size_t n, std::string_view *result,
	              char *scratch) const
	{
		if (offset > contents.size())
		{
			return Status::invalidArgument("invalid Read offset");
		}

		if (offset + n > contents.size())
		{
			n = contents.size() - offset;
		}

		memcpy(scratch, &contents[offset], n);
		*result = std::string_view(scratch, n);
		return Status::OK();
	}

private:
	std::string contents;
};

typedef std::map<std::string, std::string, STLLessThan> KVMap;

class Constructor
{
public:
	explicit Constructor(const Comparator *cmp) 
	:data(STLLessThan(cmp)) { }
	virtual ~Constructor() { }

	void add(const std::string &key, const std::string_view &value)
	{
		data[key] =  std::string(value.data(), value.size());
	}

	// Finish constructing the data structure with all the keys that have
	// been added so far.  Returns the keys in sorted order in "*keys"
	// and stores the key/value pairs in "*kvmap"
	void finish(const Options &options,
			  std::vector<std::string> *keys,
			  KVMap *kvmap)
	{
		kvmap->clear();
		*kvmap = data;
		keys->clear();
		for (auto &it : data)
		{
			keys->push_back(it.first);
		}

		data.clear();
		Status s = finishImpl(options, *kvmap);
		assert(s.ok());
	}

	// Construct the data structure from the data in "data"
	virtual Status finishImpl(const Options &options, const KVMap &data, bool store = true) = 0;
	virtual const KVMap &getData() { return data; }
	virtual std::shared_ptr<Iterator> newIterator() const = 0;

private:
	KVMap data;
};

class BlockConstructor : public Constructor
{
public:
	explicit BlockConstructor(const Comparator *cmp)
	:Constructor(cmp),
	comparator(cmp),
	block(nullptr)
	{
		
	}
	
	~BlockConstructor() 
	{
	
	}
	
	virtual std::shared_ptr<Iterator> newIterator() const 
	{
		return block->newIterator(comparator);
	}
	 
	virtual Status finishImpl(const Options& options, const KVMap &data, bool store = true)
	{
		BlockBuilder builder(&options);

		for (auto &it : data)
		{
			builder.add(it.first, it.second);
		}

		content = builder.finish();
		BlockContents contents;
		contents.data = content;
		contents.cachable = false;
		contents.heapAllocated = false;
		block.reset(new Block(contents));
		return Status::OK();	 
	}
private:
	const Comparator *comparator;
	std::string content;
	std::shared_ptr<Block> block;
	BlockConstructor();
};

class TableConstructor : public Constructor
{
public:
	TableConstructor(const Comparator *cmp)
	  : Constructor(cmp),
		filename("TableConstructor"),
		source(nullptr)
	{

	}

	~TableConstructor()
	{

	}

	virtual Status finishImpl(const Options& options, const KVMap &data, bool store = true)
	{
		table.reset();
		source.reset();

		Status s;
		if (store)
		{
			std::shared_ptr<StringSink> sink(new StringSink());
 			TableBuilder builder(options, sink);
 			for (auto it = data.begin(); it != data.end();  ++it)
			{
				builder.add(it->first, it->second);
				assert(builder.status().ok());
			}

			s = builder.finish();
			assert(s.ok());
			assert(sink->getContents().size() == builder.fileSize());
			source = std::shared_ptr<StringSource>(new StringSource(sink->getContents()));
			Options tableOptions;
			tableOptions.comparator = options.comparator;
			return Table::open(tableOptions, source, sink->getContents().size(), table);
		}
		else
		{
			std::shared_ptr<WritableFile> wfile;
			s = options.env->newWritableFile(filename, wfile);
			assert(s.ok());

			
			TableBuilder builder(options, wfile);
			for (auto it = data.begin(); it != data.end();  ++it)
			{
				builder.add(it->first, it->second);
				assert(builder.status().ok());
			}

			s = builder.finish();
			assert(s.ok());
			s = wfile->sync();
			assert(s.ok());
			s = wfile->close();
			assert(s.ok());

			uint64_t size;
			s = options.env->getFileSize(filename,&size);
			assert (s.ok());
			assert(size == builder.fileSize());
			
			std::shared_ptr<RandomAccessFile> mfile = nullptr;
			s = options.env->newRandomAccessFile(filename, mfile);
			assert(s.ok());
			return Table::open(options, mfile, builder.fileSize(), table);
		}
	}

	virtual std::shared_ptr<Iterator> newIterator() const 
	{
		return table->newIterator(ReadOptions());
	}
private:
	  std::shared_ptr<Table> table;
	  std::shared_ptr<StringSource> source;
	  std::string filename;
};

class KeyConvertingIterator : public Iterator
{
public:
	explicit KeyConvertingIterator(const std::shared_ptr<Iterator> &iter)
	:iter(iter)
	{
	
	
	}
	virtual ~KeyConvertingIterator()
	{
		
	}

	virtual bool valid() const { return iter->valid(); }
	virtual void seek(const std::string_view &target) 
	{
		ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
		std::string encoded;
		appendInternalKey(&encoded, ikey);
		iter->seek(encoded);
	}
	
	virtual void seekToFirst() { iter->seekToFirst(); }
	virtual void seekToLast() { iter->seekToLast(); }
	virtual void next() { iter->next(); }
	virtual void prev() { iter->prev(); }

	virtual std::string_view key() const 
	{
		assert(valid());
		ParsedInternalKey key;
		if (!parseInternalKey(iter->key(), &key)) 
		{
			s = Status::corruption("malformed internal key");
			return std::string_view("corrupted key");
		}
		return key.userKey;
	}

	virtual std::string_view value() const { return iter->value(); }
	virtual Status status() const 
	{
		return s.ok() ? iter->status() : s;
	}

  	virtual void registerCleanup(const std::any &arg)  { }
private:
	mutable Status s;
	std::shared_ptr<Iterator> iter;
	// No copying allowed
	KeyConvertingIterator(const KeyConvertingIterator&);
	void operator=(const KeyConvertingIterator&);
};

class MemTableConstructor : public Constructor
{
public:
	explicit MemTableConstructor(const Comparator *cmp)
	: Constructor(cmp),
	icmp(cmp),
	memtable(new MemTable(icmp))
	{
	
	}

	virtual Status finishImpl(const Options &options, const KVMap &data, bool store = true) 
	{
		memtable.reset(new MemTable(icmp));
		int seq = 1;
		for (auto &it : data)
		{
			memtable->add(seq, kTypeValue, it.first, it.second);
			seq++;
		}
		return Status::OK();
	}
	 
	virtual std::shared_ptr<Iterator> newIterator() const 
	{
		std::shared_ptr<Iterator> iter(new KeyConvertingIterator(memtable->newIterator()));
		return iter;
	}

private:
	InternalKeyComparator icmp;
	std::shared_ptr<MemTable> memtable;
};

enum TestType
{
	TABLE_TEST,
	BLOCK_TEST,
	MEMTABLE_TEST,
	DB_TEST
};

struct TestArgs
{
	TestType type;
	bool reverseCompare;
	int restartInterval;
};

static const TestArgs kTestArgList[] =
{
	{ TABLE_TEST, false, 16 },
	{ TABLE_TEST, false, 1 },
	{ TABLE_TEST, false, 1024 },
	{ TABLE_TEST, true, 16 },
	{ TABLE_TEST, true, 1 },
	{ TABLE_TEST, true, 1024 },
	{ BLOCK_TEST, false, 16 },
	{ BLOCK_TEST, false, 1 },
	{ BLOCK_TEST, false, 1024 },
	{ BLOCK_TEST, true, 16 },
	{ BLOCK_TEST, true, 1 },
	{ BLOCK_TEST, true, 1024 },
	
	// Restart interval does not matter for memtables
	{ MEMTABLE_TEST, false, 16 },
	{ MEMTABLE_TEST, true, 16 },
};

static const int kNumTestArgs = sizeof(kTestArgList) / sizeof(kTestArgList[0]);

class Harness
{
public:
	Harness()
	:constructor(nullptr)
	{

	}

	void init(const TestArgs &args)
	{
		constructor.reset();
		options = Options();
		options.blockRestartInterval = args.restartInterval;
		options.blockSize = 256;

		if (args.reverseCompare) 
		{
			reverseCmp = std::shared_ptr<ReverseKeyComparator>(new ReverseKeyComparator());
			options.comparator = reverseCmp;
		}
		else
		{
			reverseCmp.reset();
		}
		
		switch (args.type) 
		{
			case TABLE_TEST:
				constructor = std::shared_ptr<TableConstructor>(new TableConstructor(options.comparator.get()));
				break;
			case BLOCK_TEST:
				constructor = std::shared_ptr<BlockConstructor>(new BlockConstructor(options.comparator.get()));
				break;
			case MEMTABLE_TEST:
				constructor = std::shared_ptr<MemTableConstructor>(new MemTableConstructor(options.comparator.get()));
				break;
		}
	}

	~Harness()
	{

	}

	void add(const std::string &key, const std::string &value) 
	{
		constructor->add(key, value);
	}

	std::string toString(const KVMap& data, const KVMap::const_iterator &it) 
	{
		if (it == data.end()) 
		{
			return "END";
		} 
		else 
		{
			return "'" + it->first + "->" + it->second + "'";
		}
	}

	std::string toString(const KVMap &data,
	                   const KVMap::const_reverse_iterator &it) 
	{
		if (it == data.rend()) 
		{
			return "END";
		} 
		else 
		{
			return "'" + it->first + "->" + it->second + "'";
		}
	}
	
	std::string toString(const std::shared_ptr<Iterator> &it) 
	{
		if (!it->valid()) 
		{
			return "END";
		} 
		else 
		{
			key = it->key();
			value = it->value();
			return "'" + key + "->" +value + "'";
		}
	}
	
	std::string pickRandomKey(const std::vector<std::string> &keys) 
	{
		if (keys.empty())
		{
			return "foo";
		}
		else
		{
			std::default_random_engine engine;
			const int index = engine() % keys.size();
			std::string result = keys[index];
			switch(engine() % 3)
			{
				case 0:
				{
					break;
				}
				case 1:
				{
					// Attempt to return something smaller than an existing key
					if (result.size() > 0 && result[result.size() - 1] > '\0') 
					{
						result[result.size() - 1];
					}
					break;
				}
				case 2:
				{
					// Return something larger than an existing key
	         			increment(options.comparator.get(), &result);
					break;
				}
			}
			return result;
		}
	}
	
	void test() 
	{
		std::vector<std::string> keys;
		KVMap data(options.comparator.get());
		constructor->finish(options, &keys, &data);
		testForwardScan(keys, data);
		testBackwardScan(keys, data);
		testRandomAccess(keys, data);
	}
	
	void testForwardScan(const std::vector<std::string> &keys,
				   const KVMap &data) 
	{
		std::shared_ptr<Iterator> iter = constructor->newIterator();
		assert(!iter->valid());
		iter->seekToFirst();

		for (KVMap::const_iterator it = data.begin();
			it != data.end(); ++it)
		{
			std::string k = toString(data, it);
			std::string k1 = toString(iter);

			assert(k == k1);
			iter->next();
		}
		assert(!iter->valid());
	}

	void testBackwardScan(const std::vector<std::string> &keys,
	                const KVMap& data) 
	{
		std::shared_ptr<Iterator> iter = constructor->newIterator();
		assert(!iter->valid());
		iter->seekToLast();

		for (KVMap::const_reverse_iterator it = data.rbegin();
			it != data.rend(); ++it) 
		{
			std::string k = toString(data, it);
			std::string k1 = toString(iter);

			assert(k == k1);
			iter->prev();
		}
		assert(!iter->valid());
	}
	
	void testRandomAccess(const std::vector<std::string> &keys,
					const KVMap &data) 
	{
		static const bool kVerbose = false;
		std::shared_ptr<Iterator> iter = constructor->newIterator();
		assert(!iter->valid());
		KVMap::const_iterator it = data.begin();
		if (kVerbose) fprintf(stderr, "---\n");
		
		for (int i = 0; i < 200; i++)
		{
			std::default_random_engine engine;
			const int toss = engine() % 5;
			switch(toss)
			{
				case 0:
				{
					if (iter->valid())
					{
						if (kVerbose) fprintf(stderr, "Next\n");
						iter->next();
						++it;
						std::string k = toString(data, it);
						std::string k1 = toString(iter);
						assert(k == k1);
					}
					break;
				}
				case 1:
				{
					if (kVerbose) fprintf(stderr, "SeekToFirst\n");
					iter->seekToFirst();
					it = data.begin();
					
					std::string k = toString(data, it);
					std::string k1 = toString(iter);
					assert(k == k1);
					break;
				}
				case 2:
				{
					std::string key = pickRandomKey(keys);
					it = data.lower_bound(key);
					if (kVerbose) fprintf(stderr, "Seek '%s'\n",
					                   key.c_str());
					iter->seek(std::string_view(key));
					std::string k = toString(data, it);
					std::string k1 = toString(iter);
					assert(k == k1);
					break;
				}
				case 3:
				{
					if (iter->valid())
					{
						if (kVerbose) fprintf(stderr, "Prev\n");
						iter->prev();
						if (it == data.begin())
						{
							it = data.end();
						}
						else
						{
							--it;
						}
						
						std::string k = toString(data, it);
						std::string k1 = toString(iter);
						assert(k == k1);
					}
					break;
				}
				case 4:
				{
					if (kVerbose) fprintf(stderr, "SeekToLast\n");
					iter->seekToLast();
					if (keys.empty()) 
					{
						it = data.end();
					}
					else
					{
						std::string last = data.rbegin()->first;
						it = data.lower_bound(last);
						std::string k = toString(data, it);
						std::string k1 = toString(iter);
						assert(k == k1);
					}
					break;
				}
			}
		}
	}
							
private:
	Options options;
	std::shared_ptr<Constructor> constructor;
	std::string key;
	std::string value;
};

/*
int main()
{	
	// Test the empty key
	for (int i = 0; i < kNumTestArgs; i++) 
	{
		Harness arness ;
		arness.init(kTestArgList[i]);
		arness.add("k", "v");
		arness.add("k", "v");
		arness.add("k", "v0");
		arness.add("k1", "v1");
		arness.add("k2", "v2");
		arness.test();
	}
	return 0;
}
*/
