#include <unistd.h>
#include <memory>
#include <fstream>
#include <iostream>
#include <unordered_map>
#include <map>
#include <vector>
#include <set>
#include <assert.h>
#include <algorithm>

int32_t index = 0;
const size_t kMaxSize = 10 * 1000 * 1000;

class Sharder
{
public:
	explicit Sharder(int32_t nbuckets)
	{
		for (int32_t i = 0; i < nbuckets; ++i)
		{
			char buf[256];
			snprintf(buf, sizeof buf, "shard-%05d-of-%05d", i, nbuckets);
			std::shared_ptr<std::ofstream> in(new std::ofstream(buf));
			buckets.push_back(in);
		}
		assert(buckets.size() == static_cast<size_t>(nbuckets));
	}

	void output(const std::string &key, const std::string &value)
	{
		size_t idx = std::hash<std::string>()(key) % buckets.size();
		*buckets[idx] << key << '\t' << value << '\n';
	}

protected:
	std::vector<std::shared_ptr<std::ofstream>> buckets;
private:
	Sharder(const Sharder&);
	void operator=(const Sharder&);
};

void shard(int32_t nbuckets, int32_t argc, char *argv[])
{
	Sharder sharder(nbuckets);
	for (int32_t i = 1; i < argc; ++i)
	{
		std::cout << "  processing input file " << argv[i] << std::endl;
		std::map<std::string, std::string> shareds;
		std::ifstream in(argv[i]);
		while (in && !in.eof())
		{
			shareds.clear();
			std::string str;
			while (in >> str)
			{
				shareds.insert(std::make_pair(str, str));
				if (shareds.size() > kMaxSize)
				{
					std::cout << "    split" << std::endl;
					break;
				}
			}

			for (const auto &kv : shareds)
			{
				sharder.output(kv.first, kv.second);
			}
		}
	}
	std::cout << "shuffling done" << std::endl;
}

// ======= sortShareds =======

std::map<std::string, std::string> readShard(int32_t idx, int32_t nbuckets)
{
	std::map<std::string, std::string> shareds;

	char buf[256];
	snprintf(buf, sizeof buf, "shard-%05d-of-%05d", idx, nbuckets);
	std::cout << "  reading " << buf << std::endl;
	{
		std::ifstream in(buf);
		std::string line;

		while (getline(in, line))
		{
			size_t tab = line.find('\t');
			assert(tab != std::string::npos);
			std::string key(line.c_str(), line.c_str() + tab);
			std::string value(line.c_str() + tab + 1, line.c_str() + line.size());
			shareds.insert(std::make_pair(key, value));
		}
	}

	::unlink(buf);
	return shareds;
}

void sortShareds(const int32_t nbuckets)
{
	for (int32_t i = 0; i < nbuckets; ++i)
	{
		// std::cout << "  sorting " << std::endl;
		std::map<std::string, std::string> shareds;
		for (const auto &entry : readShard(i, nbuckets))
		{
			shareds.insert(std::make_pair(entry.first, entry.second));
		}

		char buf[256];
		snprintf(buf, sizeof buf, "count-%05d-of-%05d", i, nbuckets);
		std::ofstream out(buf);
		std::cout << "  writing " << buf << std::endl;
		for (auto &it : shareds)
		{
			out << it.first << '\t' << it.second << '\n';
		}
	}
	std::cout << "reducing done" << std::endl;
}

// ======= merge =======

class Source  // copyable
{
public:
	explicit Source(const std::shared_ptr<std::ifstream> &in)
		: in(in)
	{

	}

	bool next()
	{
		std::string line;
		if (getline(*(in.get()), line))
		{
			size_t tab = line.find('\t');
			if (tab != std::string::npos)
			{
				std::string key(line.c_str(), line.c_str() + tab);
				this->key = key;
				std::string(line.c_str() + tab + 1, line.c_str() + line.size());
				this->value = value;
				return true;
			}
		}
		return false;
	}

	bool operator<(const Source &rhs) const
	{
		return key < rhs.key;
	}
	
	void retrieve()
	{
		index += key.size() + 3 + value.size() + sizeof(int32_t)
	}

	void outputTo(std::ostream &out, int32_t index) const
	{
		out << key << '\t' << value << '\t' << std::to_string(index) << '\n';
		retrieve();
	}

	std::shared_ptr<std::ifstream> in;
	std::string key;
	std::string value;
};

void merge(const int32_t nbuckets)
{
	std::vector<std::shared_ptr<std::ifstream>> inputs;
	std::vector<Source> keys;

	for (int32_t i = 0; i < nbuckets; ++i)
	{
		char buf[256];
		snprintf(buf, sizeof buf, "count-%05d-of-%05d", i, nbuckets);
		std::shared_ptr<std::ifstream> in(new std::ifstream(buf));
		inputs.push_back(in);
		Source rec(in);
		if (rec.next())
		{
			keys.push_back(rec);
		}
		::unlink(buf);
	}

	std::ofstream out("output");
	std::make_heap(keys.begin(), keys.end());

	while (!keys.empty())
	{
		std::pop_heap(keys.begin(), keys.end());
		keys.back().outputTo(out, index);
	
		if (keys.back().next())
		{
			std::push_heap(keys.begin(), keys.end());
		}
		else
		{
			keys.pop_back();
		}
	}
}

int32_t main(int32_t argc, char *argv[])
{
	int32_t nbuckets = 10;
	shard(nbuckets, argc, argv);
	sortShareds(nbuckets);
	merge(nbuckets);
}
