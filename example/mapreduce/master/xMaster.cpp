#include <fstream>
#include <iostream>
#include <unordered_map>
#include <string>
#include <stdlib.h>
#include <vector>
#include <functional>
#include <algorithm>
#include <memory>
#include <condition_variable>
#include <assert.h>
#include <algorithm>
#include <unistd.h>

const size_t kMaxSize = 10 * 1000 * 1000;

class xMaster
{
public:
	xMaster(int buckets)
	{
			for(int i = 0 ; i < buckets; i++)
			{
				char buf[256];
				snprintf(buf,sizeof buf,"shard-%05d-of-%05d", i, buckets);
				std::shared_ptr<std::ofstream> pf (new std::ofstream(buf));
				ofbuckets.push_back(pf);
			}
			assert(ofbuckets.size() == static_cast<size_t>(buckets));
	}

	  void output(const std::string& word, int64_t count)
	  {
	    size_t idx = std::hash<std::string>()(word) % ofbuckets.size();
	    *(ofbuckets[idx]) << word << '\t' << count << '\n';
	  }

private:
	std::vector<std::shared_ptr<std::ofstream>>  ofbuckets;
};


void shard(int buckets, int argc, char* argv[])
{
	xMaster sharder(buckets);
	for(int i = 1; i < argc; ++i)
	{
		std::cout<<"  processing input file "<<argv[i]<<std::endl;
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

			for (const auto& kv : counts)
			{
				sharder.output(kv.first, kv.second);
			}

		}
	}

	std::cout << "shuffling done" << std::endl;
}


std::unordered_map<std::string, int64_t> readShard(int idx, int nbuckets)
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
void sortShards(const int buckets)
{
	for (int i = 0; i < buckets; ++i)
	{
		// std::cout << "  sorting " << std::endl;
		std::vector<std::pair<int64_t, std::string>> counts;
		for (const auto& entry : readShard(i, buckets))
		{
			counts.push_back(make_pair(entry.second, entry.first));
		}
		std::sort(counts.begin(), counts.end());

		char buf[256];
		snprintf(buf, sizeof buf, "count-%05d-of-%05d", i, buckets);
		std::ofstream out(buf);
		std::cout << "  writing " << buf << std::endl;
		for (auto it = counts.rbegin(); it != counts.rend(); ++it)
		{
		  out << it->first << '\t' << it->second << '\n';
		}
	}

  std::cout << "reducing done" << std::endl;
}


class xSource  // copyable
{
 public:
  xSource(const std::shared_ptr<std::istream> &in)
    : in(in),
      count(0)
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
        count = strtol(line.c_str(), nullptr, 10);
        if (count > 0)
        {
          word = line.substr(tab+1);
          return true;
        }
      }
    }
    return false;
  }

  bool operator<(const xSource& rhs) const
  {
    return count < rhs.count;
  }

  void outputTo(std::ostream& out) const
  {
    out << count << '\t' << word << '\n';
  }

 private:
  std::shared_ptr<std::istream> in;
  int64_t count;
  std::string word;
};


void merge(const int buckets)
{
	std::vector<std::shared_ptr<std::ifstream>> inputs;
	std::vector<xSource> keys;

	for (int i = 0; i < buckets; ++i)
	{
		char buf[256];
		snprintf(buf, sizeof buf, "count-%05d-of-%05d", i, buckets);
		std::shared_ptr<std::ifstream> shif (new std::ifstream(buf));
		inputs.push_back(shif);
		xSource rec(inputs.back());
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


int main(int argc, char* argv[])
{
	int buckets = 10;
	shard(buckets,argc,argv);
	sortShards(buckets);
	merge(buckets);
	return 0;
}
