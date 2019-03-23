#pragma once

#include "all.h"
#include "tcpconnection.h"
#include "tcpserver.h"
#include "zmalloc.h"
#include "object.h"

class Item;

typedef std::shared_ptr <Item> ItemPtr;
typedef std::shared_ptr<const Item> ConstItemPtr;

class Item {
public:
    enum UpdatePolicy {
        kInvalid,
        kSet,
        kAdd,
        kReplace,
        kAppend,
        kPrepend,
        kCas,
    };

    static ItemPtr makeItem(std::string_view keyArg, uint32_t flagsArg,
                            int exptimeArg, int valuelen, uint64_t casArg) {
        return std::make_shared<Item>(keyArg, flagsArg, exptimeArg, valuelen, casArg);
    }


    Item(std::string_view keyArg,
         uint32_t flagsArg,
         int exptimeArg,
         int valuelen,
         uint64_t casArg);

    ~Item() {
        zfree(data);
    }

    int getRelExptime() const { return relExptime; }

    uint32_t getFlags() const { return flags; }

    const char *value() const { return data + keyLen; }

    size_t valueLength() const { return valueLen; }

    uint64_t getCas() const { return cas; }

    void setCas(uint64_t casArg) { cas = casArg; }

    size_t neededBytes() const;

    size_t getHash() const { return hash; }

    std::string_view getKey() const { return std::string_view(data, keyLen); }

    void append(const char *data, size_t len);

    void output(Buffer *out, bool needCas = false) const;

    void resetKey(std::string_view k);

    bool endsWithCRLF() const {
        return receivedBytes == totalLen()
               && data[totalLen() - 2] == '\r'
               && data[totalLen() - 1] == '\n';
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

class MemcacheServer;

class Connect : public std::enable_shared_from_this<Connect> {
public:
    Connect(MemcacheServer *owner, const TcpConnectionPtr &conn)
            : owner(owner),
              conn(conn),
              state(kNewCommand),
              protocol(kAscii),
              noreply(false),
              policy(Item::kInvalid),
              bytesToDiscard(0),
              needle(Item::makeItem(kLongestKey, 0, 0, 2, 0)),
              bytesRead(0),
              requestsProcessed(0) {
        conn->setMessageCallback(std::bind(&Connect::onMessage, this, std::placeholders::_1, std::placeholders::_2));
    }

    void onMessage(const TcpConnectionPtr &conn, Buffer *buf);

    void onWriteComplete(const TcpConnectionPtr &conn);

    void receiveValue(Buffer *buf);

    void discardValue(Buffer *buf);

    bool processRequest(std::string_view request);

    void resetRequest();

    void reply(std::string_view msg);

    struct Reader;

    bool doUpdate(std::vector<std::string_view>::iterator &beg, std::vector<std::string_view>::iterator end);

    void doDelete(std::vector<std::string_view>::iterator &beg, std::vector<std::string_view>::iterator end);

private:
    enum State {
        kNewCommand,
        kReceiveValue,
        kDiscardValue,
    };

    enum Protocol {
        kAscii,
        kBinary,
        kAuto,
    };

    MemcacheServer *owner;
    TcpConnectionPtr conn;
    State state;
    Protocol protocol;
    std::string command;
    bool noreply;
    Item::UpdatePolicy policy;
    ItemPtr currItem;
    size_t bytesToDiscard;
    ItemPtr needle;
    Buffer outputBuf;

    size_t bytesRead;
    size_t requestsProcessed;
    static std::string kLongestKey;
};

const int kLongestKeySize = 250;
std::string Connect::kLongestKey(kLongestKeySize, 'x');
typedef std::shared_ptr <Connect> SessionPtr;

class MemcacheServer {
public:
    struct Options {
        uint16_t port;
        std::string ip;
    };

    MemcacheServer(EventLoop *loop, const Options &op);

    ~MemcacheServer();


    void setThreadNum(int threads) { server.setThreadNum(threads); }

    void quit();

    void init();

    void start();

    void stop();

    time_t getStartTime() const { return startTime; }

    bool storeItem(const ItemPtr &item, Item::UpdatePolicy policy, bool *exists);

    ConstItemPtr getItem(const ConstItemPtr &key) const;

    bool deleteItem(const ConstItemPtr &key);

private:
    void onConnection(const TcpConnectionPtr &conn);

    EventLoop *loop;
    std::unordered_map <int32_t, SessionPtr> sessions;
    Options ops;
    const time_t startTime;
    std::mutex mtx;
    std::atomic <int64_t> cas;

    struct Hash {
        size_t operator()(const ConstItemPtr &x) const {
            return x->getHash();
        }
    };

    struct Equal {
        bool operator()(const ConstItemPtr &x, const ConstItemPtr &y) const {
            return x->getKey() == y->getKey();
        }
    };

    typedef std::unordered_set <ConstItemPtr, Hash, Equal> ItemMap;
    struct MapWithLock {
        ItemMap items;
        mutable std::mutex mutex;
    };

    const static int kShards = 4096;
    std::array <MapWithLock, kShards> shards;
    TcpServer server;

};

