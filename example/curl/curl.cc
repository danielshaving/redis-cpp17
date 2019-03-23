#include "httpcurl.h"

static void dummy(const std::shared_ptr <Channel> &) {

}

Request::Request(Curl *owner, const char *url, const char *data, size_t len)
        : owner(owner),
          curl(curl_easy_init()) {
    setopt(CURLOPT_URL, url);
    setopt(CURLOPT_WRITEFUNCTION, &Request::writeData);
    setopt(CURLOPT_WRITEDATA, this);
    setopt(CURLOPT_HEADERFUNCTION, &Request::headerData);
    setopt(CURLOPT_HEADERDATA, this);
    setopt(CURLOPT_PRIVATE, this);
    setopt(CURLOPT_USERAGENT, "curl");
    setopt(CURLOPT_POST, 1L);
    setopt(CURLOPT_POSTFIELDS, data);
    setopt(CURLOPT_POSTFIELDSIZE, len);

    LOG_DEBUG << curl << " " << url;
    curl_multi_add_handle(owner->getCurlm(), curl);
}

Request::Request(Curl *owner, const char *url)
        : owner(owner),
          curl(curl_easy_init()) {
    setopt(CURLOPT_URL, url);
    setopt(CURLOPT_WRITEFUNCTION, &Request::writeData);
    setopt(CURLOPT_WRITEDATA, this);
    setopt(CURLOPT_HEADERFUNCTION, &Request::headerData);
    setopt(CURLOPT_HEADERDATA, this);
    setopt(CURLOPT_PRIVATE, this);
    setopt(CURLOPT_USERAGENT, "curl");

    LOG_DEBUG << curl << " " << url;
    curl_multi_add_handle(owner->getCurlm(), curl);
}

Request::~Request() {
    assert(!channel || channel->isNoneEvent());
    curl_multi_remove_handle(owner->getCurlm(), curl);
    curl_easy_cleanup(curl);
}

void Request::headerOnly() {
    setopt(CURLOPT_NOBODY, 1);
}

void Request::setRange(const std::string_view range) {
    setopt(CURLOPT_RANGE, range.data());
}

const char *Request::getEffectiveUrl() {
    const char *p = nullptr;
    curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &p);
    return p;
}

const char *Request::getRedirectUrl() {
    const char *p = nullptr;
    curl_easy_getinfo(curl, CURLINFO_REDIRECT_URL, &p);
    return p;
}

int Request::getResponseCode() {
    long code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    return static_cast<int>(code);
}

Channel *Request::setChannel(int fd) {
    assert(channel.get() == nullptr);
    channel.reset(new Channel(owner->getLoop(), fd));
    channel->setTie(shared_from_this());
    return channel.get();
}

void Request::removeChannel() {
    channel->disableAll();
    channel->remove();
    owner->getLoop()->queueInLoop(std::bind(dummy, channel));
    channel.reset();
}

void Request::done(int code) {
    if (doneCb) {
        doneCb(shared_from_this(), code);
    }
}

void Request::dataCallback(const char *buffer, int len) {
    if (dataCb) {
        this->buffer.append(buffer, len);
        dataCb(shared_from_this(), buffer, len);
    }
}

void Request::headerCallback(const char *buffer, int len) {
    if (headerCb) {
        headerCb(buffer, len);
    }
}

size_t Request::writeData(char *buffer, size_t size, size_t nmemb, void *userp) {
    assert(size == 1);
    Request *req = static_cast<Request *>(userp);
    req->dataCallback(buffer, static_cast<int>(nmemb));
    return nmemb;
}

size_t Request::headerData(char *buffer, size_t size, size_t nmemb, void *userp) {
    assert(size == 1);
    Request *req = static_cast<Request *>(userp);
    req->headerCallback(buffer, static_cast<int>(nmemb));
    return nmemb;
}

// ==================================================================

void Curl::initialize(Option opt) {
    curl_global_init(opt == kCURLnossl ? CURL_GLOBAL_NOTHING : CURL_GLOBAL_SSL);
}

int Curl::socketCallback(CURL *c, int fd, int what, void *userp, void *socketp) {
    Curl *curl = static_cast<Curl *>(userp);
    const char *whatstr[] = {"none", "IN", "OUT", "INOUT", "REMOVE"};
    LOG_DEBUG << "Curl::socketCallback [" << curl << "] - fd = " << fd
              << " what = " << whatstr[what];
    Request *req = nullptr;
    curl_easy_getinfo(c, CURLINFO_PRIVATE, &req);
    assert(req->getCurl() == c);
    if (what == CURL_POLL_REMOVE) {
        Channel *ch = static_cast<Channel *>(socketp);
        assert(req->getChannel() == ch);
        req->removeChannel();
        ch = nullptr;
        curl_multi_assign(curl->curlm, fd, ch);
    } else {
        Channel *ch = static_cast<Channel *>(socketp);
        if (!ch) {
            ch = req->setChannel(fd);
            ch->setReadCallback(std::bind(&Curl::onRead, curl, fd));
            ch->setWriteCallback(std::bind(&Curl::onWrite, curl, fd));
            ch->enableReading();
            curl_multi_assign(curl->curlm, fd, ch);
            LOG_TRACE << "new channel for fd=" << fd;
        }

        assert(req->getChannel() == ch);
        // update
        if (what & CURL_POLL_OUT) {
            ch->enableWriting();
        } else {
            ch->disableWriting();
        }
    }
    return 0;
}

int Curl::timerCallback(CURLM *curlm, long ms, void *userp) {
    Curl *curl = static_cast<Curl *>(userp);
    LOG_DEBUG << curl << " " << ms << " ms";
    curl->loop->runAfter(static_cast<int>(ms) / 1000.0, false, std::bind(&Curl::onTimer, curl));
    return 0;
}

Curl::Curl(EventLoop *loop)
        : loop(loop),
          curlm(curl_multi_init()),
          runningHandles(0),
          prevRunningHandles(0) {
    curl_multi_setopt(curlm, CURLMOPT_SOCKETFUNCTION, &Curl::socketCallback);
    curl_multi_setopt(curlm, CURLMOPT_SOCKETDATA, this);
    curl_multi_setopt(curlm, CURLMOPT_TIMERFUNCTION, &Curl::timerCallback);
    curl_multi_setopt(curlm, CURLMOPT_TIMERDATA, this);
}

Curl::~Curl() {
    curl_multi_cleanup(curlm);
}

RequestPtr Curl::getUrl(std::string_view url) {
    RequestPtr req(new Request(this, url.data()));
    return req;
}

RequestPtr Curl::getUrl(std::string_view url, std::string_view body) {
    RequestPtr req(new Request(this, url.data(), body.data(), body.size()));
    return req;
}

void Curl::onTimer() {
    CURLMcode rc = CURLM_OK;
    do {
        LOG_TRACE;
        rc = curl_multi_socket_action(curlm, CURL_SOCKET_TIMEOUT, 0, &runningHandles);
        LOG_TRACE << rc << " " << runningHandles;
    } while (rc == CURLM_CALL_MULTI_PERFORM);
    checkFinish();
}

void Curl::onRead(int fd) {
    CURLMcode rc = CURLM_OK;
    do {
        LOG_TRACE << fd;
        rc = curl_multi_socket_action(curlm, fd, CURL_POLL_IN, &runningHandles);
        LOG_TRACE << fd << " " << rc << " " << runningHandles;
    } while (rc == CURLM_CALL_MULTI_PERFORM);
    checkFinish();
}

void Curl::onWrite(int fd) {
    CURLMcode rc = CURLM_OK;
    do {
        LOG_TRACE << fd;
        rc = curl_multi_socket_action(curlm, fd, CURL_POLL_OUT, &runningHandles);
        LOG_TRACE << fd << " " << rc << " " << runningHandles;
    } while (rc == CURLM_CALL_MULTI_PERFORM);
    checkFinish();
}

void Curl::checkFinish() {
    if (prevRunningHandles > runningHandles || runningHandles == 0) {
        CURLMsg *msg = nullptr;
        int left = 0;
        while ((msg = curl_multi_info_read(curlm, &left)) != nullptr) {
            if (msg->msg == CURLMSG_DONE) {
                CURL *c = msg->easy_handle;
                CURLcode res = msg->data.result;
                Request *req = nullptr;
                curl_easy_getinfo(c, CURLINFO_PRIVATE, &req);
                assert(req->getCurl() == c);
                LOG_TRACE << req << " done";
                req->done(res);
            }
        }
    }
    prevRunningHandles = runningHandles;
}

