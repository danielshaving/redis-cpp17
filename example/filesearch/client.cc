#include "all.h"
#include "tcpconnection.h"
#include "tcpclient.h"
#include "eventloop.h"
#include "log.h"
#include "table.h"
#include "posix.h"
#include "tablebuilder.h"
#include "memtable.h"
#include "option.h"

const size_t kMaxSize = 10 * 1000 * 1000;
enum SaverState
{
	kNotFound,
	kFound,
	kDeleted,
	kCorrupt,
};

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
	
	std::string getKey() { MD5(key).toStr(); }
	std::string getValue() { return value; }
	void outputTo(std::ostream &out)
	{
		out << key << '\t' << value << '\n';
	}

	std::shared_ptr<std::ifstream> in;
	std::string key;
	std::string value;
};

class Client
{
public:
	Client(const char *ip, uint16_t port, int32_t argc, char *argv[])
	:ip(ip),
	port(port),
	nbuckets(10)
	{
		EventLoop loop;
		this->loop = &loop;
		TcpClient client(&loop, ip, port, nullptr);
		client.setConnectionCallback(std::bind(&Client::connectionCallback, this, std::placeholders::_1));
		client.setMessageCallback(std::bind(&Client::messageCallback, this, std::placeholders::_1, std::placeholders::_2));
		client.enableRetry();
		client.connect(true);
		this->client = &client;
		
		shard(nbuckets, argc, argv);
		sortShareds(nbuckets);
		merge(nbuckets);
	}
	
	void writeCompleteCallback(const TcpConnectionPtr &conn)
	{
		conn->startRead();
		conn->setWriteCompleteCallback(WriteCompleteCallback());
	}

	void highWaterCallback(const TcpConnectionPtr &conn, size_t bytesToSent)
	{
		LOG_INFO << " bytes " << bytesToSent;
		if (conn->outputBuffer()->readableBytes() > 0)
		{
			conn->stopRead();
			conn->setWriteCompleteCallback(
				std::bind(&Client::writeCompleteCallback, this, std::placeholders::_1));
		}
	}

	void connectionCallback(const TcpConnectionPtr &conn)
	{
		conn->setHighWaterMarkCallback(
			std::bind(&Client::highWaterCallback, this, std::placeholders::_1, std::placeholders::_2),
			1024 * 1024);
	}
	
	void cmp(const std::any &arg, const std::string_view &ikey, const std::string_view &v)
	{
		auto c = ikey.compare(v);
		if (c == 0)
		{
			state = kCorrupt;
		}
		else
		{
			state = kFound;
		}
	}

	void messageCallback(const TcpConnectionPtr &conn, Buffer *buffer)
	{
		const char *crlf;
		while ((crlf = buffer->findCRLF()) != nullptr)
		{
			// string request(buffer->peek(), crlf);
			// printf("%s\n", request.c_str());
			const char *tab = std::find(buffer->peek(), crlf, '\t');
			if (tab != crlf)
			{
				std::string key(buffer->peek(), tab);
				std::string value(tab + 1, crlf);

				Status s = table->internalGet(ReadOptions(), key, nullptr, std::bind(&Client::cmp, this,
						std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));

				if (!s.ok())
				{
					return s;
				}

				switch (state)
				{
					case kNotFound:
						files[key] = value;
					  break;      // Keep searching in other files
					case kFound:
					  break;
				}
			}
			else
			{
				LOG_WARN << "Wrong format, no tab found";
				conn->shutdown();
			}
			buffer->retrieveUntil(crlf + 2);
		}
	}
	
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

		Options options;
		std::shared_ptr<WritableFile> wfile;
		s = options.env->newWritableFile("output", wfile);
		assert(s.ok());
		TableBuilder builder(options, wfile);

		std::make_heap(keys.begin(), keys.end());
		while (!keys.empty())
		{
			std::pop_heap(keys.begin(), keys.end());
			builder.add(keys.back().getKey(), keys.back().getValue());
			assert(builder.status().ok());

			if (keys.back().next())
			{
				std::push_heap(keys.begin(), keys.end());
			}
			else
			{
				keys.pop_back();
			}
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
		s = Table::open(options, mfile, builder.fileSize(), table);
		assert(s.ok());
	}
	
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
	
	void run()
	{
		loop->run();
	}
private:
	Client(const Client&);
	void operator=(const Client&);
	
	EventLoop *loop;
	TcpClient *client;
	const char *ip;
	uint16_t port;
	int32_t nbuckets;
	std::shared_ptr<Table> table;
	std::map<std::string, std::string> files;
	Status state;

};

int main(int argc, char* argv[])
{
	Client client("127.0.0.1", 8888, argc, argv); 
	client.run();
	return 0;
}
