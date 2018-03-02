#pragma once
#include "all.h"
#include "xTcpConnection.h"
#include "xTcpServer.h"
#include "xZmalloc.h"
#include "xObject.h"

class xItem;

typedef std::shared_ptr<xItem> ItemPtr;
typedef std::shared_ptr<const xItem> ConstItemPtr;

class xItem:noncopyable
{
public:
	enum UpdatePolicy
	{
		kInvalid,
		kSet,
		kAdd,
		kReplace,
		kAppend,
		kPrepend,
		kCas,
	};

	static ItemPtr makeItem(xStringPiece keyArg,
							  uint32_t flagsArg,
							  int exptimeArg,
							  int valuelen,
							  uint64_t casArg)
	  {
		return std::make_shared<xItem>(keyArg, flagsArg, exptimeArg, valuelen, casArg);
	  }

	
	xItem(xStringPiece keyArg,
		 uint32_t flagsArg,
		 int exptimeArg,
		 int valuelen,
		 uint64_t casArg);
	
	~xItem()
	{
	 	 zfree(data);
	}


	int getRelExptime() const  {  return relExptime; }
	uint32_t getFlags() const {  return flags; }
	const char* value() const {  return data +keyLen; }
	size_t valueLength() const {  return valueLen; }
	uint64_t getCas() const{ return cas; }
	void setCas(uint64_t casArg) { cas = casArg;}
	size_t neededBytes() const;
	size_t getHash() const { return hash; }
	xStringPiece getKey()const { return xStringPiece(data,keyLen); }
	void append(const char* data, size_t len);
	void output(xBuffer* out, bool needCas = false) const;
	void resetKey(xStringPiece k);

	bool endsWithCRLF() const
	{
		return receivedBytes == totalLen()
		&& data[totalLen()-2] == '\r'
		&& data[totalLen()-1] == '\n';
	}

	
private:
	 int totalLen() const { return keyLen + valueLen; }
	 
	int keyLen;
	const uint32_t flags;
	const int relExptime;
	const int valueLen;
	int receivedBytes;
	uint64_t cas;
	size_t hash;
	char *data;	
};


class xMemcacheServer;

class xConnect : noncopyable, public std::enable_shared_from_this<xConnect>
{
public:
	xConnect(xMemcacheServer *owner,const TcpConnectionPtr & conn)
	:owner(owner),
	conn(conn),
	state(kNewCommand),
	protocol(kAscii),
	noreply(false),
	policy(xItem::kInvalid),
	bytesToDiscard(0),
	needle(xItem::makeItem(kLongestKey, 0, 0, 2, 0)),
	bytesRead(0),
	requestsProcessed(0)
	{
		 conn->setMessageCallback(std::bind(&xConnect::onMessage, this, std::placeholders::_1,std::placeholders::_2));
	}
	

	void onMessage(const TcpConnectionPtr  & conn,xBuffer *buf);

	void onWriteComplete(const TcpConnectionPtr& conn);
	void receiveValue(xBuffer* buf);
	void discardValue(xBuffer* buf);

	bool processRequest(xStringPiece request);
	void resetRequest();
	void reply(xStringPiece msg);
	struct Reader;
	bool doUpdate(std::vector<xStringPiece>::iterator &beg,std::vector<xStringPiece>::iterator end);
	void doDelete(std::vector<xStringPiece>::iterator &beg, std::vector<xStringPiece>::iterator end);
	

private:

	 enum State
	  {
	    kNewCommand,
	    kReceiveValue,
	    kDiscardValue,
	  };

	  enum Protocol
	  {
	    kAscii,
	    kBinary,
	    kAuto,
	  };

	xMemcacheServer * owner;
	TcpConnectionPtr conn;
	State state;
	Protocol protocol;
	std::string command;
	bool noreply;
	xItem::UpdatePolicy policy;
	ItemPtr currItem;
	size_t bytesToDiscard;
	ItemPtr needle;
	xBuffer outputBuf;

	size_t bytesRead;
	size_t requestsProcessed;

	static std::string kLongestKey;
	
	
};

const int kLongestKeySize = 250;

std::string xConnect::kLongestKey(kLongestKeySize, 'x');


typedef std::shared_ptr<xConnect> SessionPtr;


class xMemcacheServer :noncopyable
{
public:
	struct Options
	{
		uint16_t port;
		std::string ip;
	};

	xMemcacheServer(xEventLoop *loop,const Options & op);
	~xMemcacheServer();

	
	void setThreadNum(int threads) { server.setThreadNum(threads); }
	void quit(const std::any &context);
	void init();
	void start();
	void stop();

	time_t getStartTime() const { return startTime; }
	bool storeItem(const ItemPtr & item, xItem::UpdatePolicy policy, bool *exists);
	ConstItemPtr getItem(const ConstItemPtr & key) const;
	bool deleteItem(const ConstItemPtr & key);

private:
	void onConnection(const TcpConnectionPtr & conn);

	xEventLoop *loop;
	std::unordered_map<int32_t,SessionPtr>  sessions;
	Options ops;
	const time_t startTime;
	std::mutex mtx;
	std::atomic<int64_t> cas;

	struct xHash
	{
		size_t operator()(const ConstItemPtr & x) const 
		{
			return x->getHash();	
		}
	};

	struct xEqual
	{
		bool operator()(const ConstItemPtr & x,const ConstItemPtr & y) const
		{
			return x->getKey() == y->getKey();
		}
	};

	typedef std::unordered_set<ConstItemPtr,xHash,xEqual> ItemMap;
	
	struct MapWithLock
	{
		ItemMap items;
		mutable std::mutex mutex;
	};
	
	const static int kShards = 4096;
	std::array<MapWithLock,kShards> shards;
	xTcpServer server;
	
};

