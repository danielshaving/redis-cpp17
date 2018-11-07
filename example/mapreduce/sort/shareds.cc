/* sort word by frequency, sharding while counting version.

  1. read input file, do counting, if counts > 10M keys, write counts to 10 shard files:
	   word \t count
  2. assume each shard file fits in memory, read each shard file, accumulate counts, and write to 10 count files:
	   count \t word
  3. merge 10 count files using heap.

Limits: each shard must fit in memory.
*/
#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

#include <fstream>
#include <iostream>
#include <unordered_map>
#include <map>

const size_t kMaxSize = 10 * 1000 * 1000;

class Sharder : boost::noncopyable
{
public:
	explicit Sharder(int32_t nbuckets)
		: buckets(nbuckets)
	{
		for (int32_t i = 0; i < nbuckets; ++i)
		{
			char buf[256];
			snprintf(buf, sizeof buf, "shard-%05d-of-%05d", i, nbuckets);
			buckets.push_back(new std::ofstream(buf));
		}
		assert(buckets.size() == static_cast<size_t>(nbuckets));
	}

	void output(const std::string& word, int64_t count)
	{
		size_t idx = std::hash<std::string>()(word) % buckets.size();
		buckets[idx] << word << '\t' << count << '\n';
	}

protected:
	boost::ptr_vector<std::ofstream> buckets;
};

void shard(int32_t nbuckets, int32_t argc, char *argv[])
{
	Sharder sharder(nbuckets);
	for (int32_t i = 1; i < argc; ++i)
	{
		std::cout << "  processing input file " << argv[i] << std::endl;
		std::unordered_map<std::string, int64_t> counts;
		std::ifstream in(argv[i]);
		while (in && !in.eof())
		{
			counts.clear();
			std::string word;
			while (in >> word)
			{
				counts[word]++;
				if (counts.size() > kMaxSize)
				{
					std::cout << "    split" << std::endl;
					break;
				}
			}

			for (const auto &kv : counts)
			{
				sharder.output(kv.first, kv.second);
			}
		}
	}
	std::cout << "shuffling done" << std::endl;
}

// ======= sortShareds =======

std::unordered_map<std::string, int64_t> readShard(int32_t idx, int32_t nbuckets)
{
	std::unordered_map<std::string, int64_t> counts;

	char buf[256];
	snprintf(buf, sizeof buf, "shard-%05d-of-%05d", idx, nbuckets);
	std::cout << "  reading " << buf << std::endl;
	{
		std::ifstream in(buf);
		std::string line;

		while (getline(in, line))
		{
			size_t tab = line.find('\t');
			if (tab != std::string::npos)
			{
				int64_t count = strtol(line.c_str() + tab, nullptr, 10);
				if (count > 0)
				{
					counts[line.substr(0, tab)] += count;
				}
			}
		}
	}

	::unlink(buf);
	return counts;
}

void sortShareds(const int32_t nbuckets)
{
	for (int32_t i = 0; i < nbuckets; ++i)
	{
		// std::cout << "  sorting " << std::endl;
		std::vector<std::pair<int64_t, std::string>> counts;
		for (const auto &entry : readShard(i, nbuckets))
		{
			counts.push_back(make_pair(entry.second, entry.first));
		}
		std::sort(counts.begin(), counts.end());

		char buf[256];
		snprintf(buf, sizeof buf, "count-%05d-of-%05d", i, nbuckets);
		std::ofstream out(buf);
		std::cout << "  writing " << buf << std::endl;
		for (auto it = counts.rbegin(); it != counts.rend(); ++it)
		{
			out << it->first << '\t' << it->second << '\n';
		}
	}

	std::cout << "reducing done" << std::endl;
}

// ======= merge =======

class Source  // copyable
{
public:
	explicit Source(std::istream* in)
		: in(in),
		count(0),
		word()
	{
	}

	bool next()
	{
		std::string line;
		if (getline(*in, line))
		{
			size_t tab = line.find('\t');
			if (tab != std::string::npos)
			{
				count = strtol(line.c_str(), nullptr, 10);
				if (count > 0)
				{
					word = line.substr(tab + 1);
					return true;
				}
			}
		}
		return false;
	}

	bool operator<(const Source &rhs) const
	{
		return count < rhs.count;
	}

	void outputTo(std::ostream &out) const
	{
		out << count << '\t' << word << '\n';
	}

private:
	std::istream *in;
	int64_t count;
	std::string word;
};

void merge(const int32_t nbuckets)
{
	boost::ptr_vector<std::ifstream> inputs;
	std::vector<Source> keys;

	for (int32_t i = 0; i < nbuckets; ++i)
	{
		char buf[256];
		snprintf(buf, sizeof buf, "count-%05d-of-%05d", i, nbuckets);
		inputs.push_back(new std::ifstream(buf));
		Source rec(&inputs.back());
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
		keys.back().outputTo(out);

		if (keys.back().next())
		{
			std::push_heap(keys.begin(), keys.end());
		}
		else
		{
			keys.pop_back();
		}
	}
	std::cout << "merging done\n";
}

int32_t main(int32_t argc, char* argv[])
{
	int32_t nbuckets = 10;
	shard(nbuckets, argc, argv);
	sortShareds(nbuckets);
	merge(nbuckets);
}
