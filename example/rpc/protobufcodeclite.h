#pragma once

#include "all.h"
#include "callback.h"

namespace google {
    namespace protobuf {
        class Message;
    }
}

typedef std::shared_ptr<::google::protobuf::Message> MessagePtr;

class ProtobufCodecLite {
public:
    const static int kHeaderLen = sizeof(int32_t);
    const static int kChecksumLen = sizeof(int32_t);
    const static int kMaxMessageLen = 64 * 1024 * 1024;
    enum ErrorCode {
        kNoError = 0,
        kInvalidLength,
        kCheckSumError,
        kInvalidNameLen,
        kUnknownMessageType,
        kParseError,
    };

    typedef std::function<bool(const TcpConnectionPtr &, std::string_view)> RawMessageCallback;
    typedef std::function<void(const TcpConnectionPtr &, const MessagePtr &)> ProtobufMessageCallback;
    typedef std::function<void(const TcpConnectionPtr &, Buffer *, ErrorCode)> ErrorCallback;

    ProtobufCodecLite(const ::google::protobuf::Message *prototype, StringPiece tagArg,
                      const ProtobufMessageCallback &messageCb,
                      const RawMessageCallback &rawCb = RawMessageCallback(),
                      const ErrorCallback &errorCb = ErrorCallback())
            : prototype(prototype),
              tag(tagArg.as_string()),
              messageCallback(messageCb),
              rawCb(rawCb),
              errorCallback(errorCb),
              kMinMessageLen(tagArg.size() + kChecksumLen) {

    }

    virtual ~ProtobufCodecLite() {

    }

    const std::string &getTag() const { return tag; }

    void send(const TcpConnectionPtr &conn, const ::google::protobuf::Message &message);

    void onMessage(const TcpConnectionPtr &conn, Buffer *buf);

    virtual bool parseFromBuffer(std::string_view buf, ::google::protobuf::Message *message);

    virtual int serializeToBuffer(const ::google::protobuf::Message &message, Buffer *buf);

    static const std::string &errorCodeToString(ErrorCode errorCode);

    ErrorCode parse(const char *buf, int len, ::google::protobuf::Message *message);

    void fillEmptyBuffer(Buffer *buf, const ::google::protobuf::Message &message);

    static int32_t checkSum(const void *buf, int len);

    static bool validateCheckSum(const char *buf, int len);

    static int32_t asInt32(const char *buf);

    static void defaultErrorCallback(const TcpConnectionPtr &, Buffer *, ErrorCode);

private:
    const ::google::protobuf::Message *prototype;
    const std::string tag;
    ProtobufMessageCallback messageCallback;
    RawMessageCallback rawCb;
    ErrorCallback errorCallback;
    const int kMinMessageLen;
};

template<typename MSG, const char *TAG, typename CODEC = ProtobufCodecLite>
class ProtobufCodecLiteT {
public:
    typedef std::shared_ptr <MSG> ConcreateMessagePtr;
    typedef std::function<void(const TcpConnectionPtr &, const ConcreateMessagePtr &)> ProtobufMessageCallback;
    typedef ProtobufCodecLite::RawMessageCallback RawMessageCallback;
    typedef ProtobufCodecLite::ErrorCallback ErrorCallback;

    explicit ProtobufCodecLiteT(const ProtobufMessageCallback &messageCb,
                                const RawMessageCallback &rawCb = RawMessageCallback(),
                                const ErrorCallback &errorCb = ProtobufCodecLite::defaultErrorCallback)
            : messageCallback(messageCb),
              codec(&MSG::default_instance(),
                    TAG,
                    std::bind(&ProtobufCodecLiteT::onRpcMessage, this, std::placeholders::_1, std::placeholders::_2),
                    rawCb,
                    errorCb) {

    }

    const std::string &tag() const { return codec.getTag(); }

    void send(const TcpConnectionPtr &conn, const MSG &message) {
        codec.send(conn, message);
    }

    void onMessage(const TcpConnectionPtr &conn, Buffer *buf) {
        codec.onMessage(conn, buf);
    }

    void onRpcMessage(const TcpConnectionPtr &conn, const MessagePtr &message) {
        messageCallback(conn, std::static_pointer_cast<MSG>(message));
    }

    void fillEmptyBuffer(Buffer *buf, const MSG &message) {
        codec.fillEmptyBuffer(buf, message);
    }

private:
    ProtobufMessageCallback messageCallback;
    CODEC codec;
};






