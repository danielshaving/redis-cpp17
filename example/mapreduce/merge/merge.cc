#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/ptr_container/ptr_vector.hpp>

#include <fstream>
#include <stdio.h>

class Source
{
public:
	explicit Source(std::istream *in)
		: in(in)
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
				std::string key(line.c_str(), line.c_str() + tab);
				this->key = key;
				std::string(line.c_str() + tab + 1, line.c_str() + line.size());
				this->value = value;
				return true;
			}
		}
		return false;
	}

	bool operator < (const Source &rhs) const
	{
		return key < rhs.key;
	}

	void output(std::ostream &out)
	{
		out << key << '\t' << value << '\n';
	}

private:
	std::istream *in;
	std::string key;
	std::string value;
};

boost::asio::ip::tcp::endpoint getPort(const std::string &ipport)
{
	size_t colon = ipport.find(':');
	if (colon != std::string::npos)
	{
		std::string ip = ipport.substr(0, colon);
		uint16_t port = static_cast<uint16_t>(atoi(ipport.c_str() + colon + 1));
		return boost::asio::ip::tcp::endpoint(boost::asio::ip::address::from_string(ip), port);
	}
	else
	{
		throw std::invalid_argument("Invalid format of endpoint");
	}
}

int main(int argc, char *argv[])
{
	if (argc >= 3)
	{
		boost::ptr_vector<boost::asio::ip::tcp::iostream> inputs;
		std::vector<Source> keys;
		const int64_t topK = atoll(argv[1]);

		for (int i = 2; i < argc; ++i)
		{
			inputs.push_back(new boost::asio::ip::tcp::iostream(getPort(argv[i])));
			Source src(&inputs.back());
			if (src.next())
			{
				keys.push_back(src);
			}
		}
		printf("Connected to %zd sender(s)\n", keys.size());

		std::ofstream out("output");
		int64_t cnt = 0;
		std::make_heap(keys.begin(), keys.end());
		while (!keys.empty() && cnt < topK)
		{
			std::pop_heap(keys.begin(), keys.end());
			keys.back().output(out);
			++cnt;

			if (keys.back().next())
			{
				std::push_heap(keys.begin(), keys.end());
			}
			else
			{
				keys.pop_back();
			}
		}
		printf("merging done\n");
	}
	else
	{
		printf("Usage: %s topK ip1:port1 [ip2:port2 ...]\n", argv[0]);
	}
}

