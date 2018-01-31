#pragma once
#include "xCallback.h"

namespace google
{
	namespace protobuf
	{
		class Message;
	}
}


typedef std::shared_ptr<::google::protobuf::Message> MessagePtr;

class  xProtobufCodecLite:noncopyable
{
public:
	const static int kHeaderLen = sizeof(int32_t);
	const static int kChecksumLen = sizeof(int32_t);
	const static int kMaxMessageLen = 64*1024*1024;
	enum ErrorCode
	{
		kNoError = 0,
		kInvalidLength,
		kCheckSumError,
		kInvalidNameLen,
		kUnknownMessageType,
		kParseError,
	};

	typedef std::function<bool(const xTcpconnectionPtr &, xStringPiece)>  RawMessageCallback;
	typedef std::function<void(const xTcpconnectionPtr &, const MessagePtr &)> ProtobufMessageCallback;
	typedef std::function<void(const xTcpconnectionPtr &, xBuffer*, ErrorCode)> ErrorCallback;

	xProtobufCodecLite(const ::google::protobuf::Message *prototype,xStringPiece tagArg,
			const ProtobufMessageCallback &messageCb,
			const RawMessageCallback &rawCb = RawMessageCallback(),
			const ErrorCallback &errorCb = ErrorCallback())
	:prototype(prototype),
	 tag(tagArg.as_string()),
	 messageCallback(messageCb),
	 rawCb(rawCb),
	 errorCallback(errorCb),
	 kMinMessageLen(tagArg.size() + kChecksumLen)
	{

	}
	virtual ~xProtobufCodecLite()
	{

	}

	const std::string & getTag() const { return tag; }

	void send(const xTcpconnectionPtr &conn,const ::google::protobuf::Message &message);
	void onMessage(const xTcpconnectionPtr &conn,xBuffer *buf);
	virtual bool parseFromBuffer(xStringPiece buf,::google::protobuf::Message *message);
	virtual int serializeToBuffer(const ::google::protobuf::Message &message, xBuffer *buf);

	static const std::string & errorCodeToString(ErrorCode errorCode);

	ErrorCode parse(const char *buf,int len, ::google::protobuf::Message *message);
	void fillEmptyBuffer(xBuffer *buf,const ::google::protobuf::Message  &message);

	static int32_t checkSum(const void *buf,int len);
	static bool validateCheckSum(const char* buf, int len);
	static int32_t asInt32(const char *buf);
	static void defaultErrorCallback(const xTcpconnectionPtr &,xBuffer *,ErrorCode);

private:
	const ::google::protobuf::Message* prototype;
	const std::string tag;
	ProtobufMessageCallback messageCallback;
	RawMessageCallback rawCb;
	ErrorCallback errorCallback;
	const int kMinMessageLen;
};


template<typename MSG, const char* TAG, typename CODEC = xProtobufCodecLite>
class xProtobufCodecLiteT
{
public:
	typedef std::shared_ptr<MSG> ConcreateMessagePtr;
	typedef std::function<void (const xTcpconnectionPtr &,const ConcreateMessagePtr &)> ProtobufMessageCallback;
	typedef xProtobufCodecLite::RawMessageCallback RawMessageCallback;
	typedef xProtobufCodecLite::ErrorCallback ErrorCallback;

	explicit xProtobufCodecLiteT(const ProtobufMessageCallback& messageCb,
							  const RawMessageCallback& rawCb = RawMessageCallback(),
							  const ErrorCallback& errorCb = xProtobufCodecLite::defaultErrorCallback)
	: messageCallback(messageCb),
	  codec(&MSG::default_instance(),
			 TAG,
			 std::bind(&xProtobufCodecLiteT::onRpcMessage, this, std::placeholders::_1,std::placeholders::_2),
			 rawCb,
			 errorCb)
	{

	}

	const std::string& tag() const { return codec.getTag(); }
	void send(const xTcpconnectionPtr& conn, const MSG& message)
	{
		codec.send(conn, message);
	}

	void onMessage(const xTcpconnectionPtr& conn,xBuffer* buf)
	{
		codec.onMessage(conn, buf);
	}

	void onRpcMessage(const xTcpconnectionPtr& conn, const MessagePtr& message)
	{
		messageCallback(conn, std::static_pointer_cast<MSG>(message));
	}

	void fillEmptyBuffer(xBuffer* buf, const MSG& message)
	{
		codec.fillEmptyBuffer(buf, message);
	}



private:
	ProtobufMessageCallback messageCallback;
	CODEC codec;
};






