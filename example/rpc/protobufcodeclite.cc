#include <google/protobuf/message.h>
#include <google.h>
#include <zlib.h>
#include "protobufcodeclite.h"
#include "log.h"
#include "tcpconnection.h"

namespace
{
	int ProtobufVersionCheck()
	{
		GOOGLE_PROTOBUF_VERIFY_VERSION;
		return 0;
	}
	int __attribute__ ((unused)) dummy = ProtobufVersionCheck();
}


void ProtobufCodecLite::send(const TcpConnectionPtr& conn,
                             const ::google::protobuf::Message &message)
{
	// FIXME: serialize to TcpConnection::outputBuffer()
	Buffer buf;
	fillEmptyBuffer(&buf,message);
	conn->send(&buf);
}

void ProtobufCodecLite::fillEmptyBuffer(Buffer* buf,const ::google::protobuf::Message& message)
{
	assert(buf->readableBytes() == 0);
	buf->append(tag);
	int byte_size = serializeToBuffer(message, buf);

	int32_t checksum = checkSum(buf->peek(), static_cast<int>(buf->readableBytes()));
	buf->appendInt32(checksum);
	assert(buf->readableBytes() == tag.size() + byte_size + kChecksumLen); (void) byte_size;
	int32_t len = static_cast<int32_t>(buf->readableBytes());
	buf->prepend(&len, sizeof len);
}

void ProtobufCodecLite::onMessage(const TcpConnectionPtr& conn,Buffer *buf)
{
	while (buf->readableBytes() >= static_cast<uint32_t>(kMinMessageLen+kHeaderLen))
	{
		const int32_t len = buf->peekInt32();
		if (len > kMaxMessageLen || len < kMinMessageLen)
		{
			errorCallback(conn, buf,kInvalidLength);
			break;
		}
		else if (buf->readableBytes() >= kHeaderLen+len)
		{
			if (rawCb && !rawCb(conn,std::string_view(buf->peek(),kHeaderLen+len)))
			{
				buf->retrieve(kHeaderLen+len);
				continue;
			}

			MessagePtr message(prototype->New());
			ErrorCode errorCode = parse(buf->peek()+kHeaderLen,len,message.get());
			if (errorCode == kNoError)
			{
				messageCallback(conn,message);
				buf->retrieve(kHeaderLen+len);
			}
			else
			{
				errorCallback(conn,buf,errorCode);
				break;
			}
		}
		else
		{
			break;
		}
	}
}

bool ProtobufCodecLite::parseFromBuffer(std::string_view buf,::google::protobuf::Message *message)
{
	return message->ParseFromArray(buf.data(),buf.size());
}

int ProtobufCodecLite::serializeToBuffer(const ::google::protobuf::Message &message,Buffer *buf)
{
	GOOGLE_DCHECK(message.IsInitialized()) << InitializationErrorMessage("serialize", message);

	int byteSize = message.ByteSize();
	buf->ensureWritableBytes(byteSize + kChecksumLen);

	uint8_t *start = reinterpret_cast<uint8_t*>(buf->beginWrite());
	uint8_t *end = message.SerializeWithCachedSizesToArray(start);
	if (end - start != byteSize)
	{
		ByteSizeConsistencyError(byteSize, message.ByteSize(), static_cast<int>(end - start));
	}

	buf->hasWritten(byteSize);
	return byteSize;
}

namespace
{
	const std::string kNoErrorStr = "NoError";
	const std::string kInvalidLengthStr = "InvalidLength";
	const std::string kCheckSumErrorStr = "CheckSumError";
	const std::string kInvalidNameLenStr = "InvalidNameLen";
	const std::string kUnknownMessageTypeStr = "UnknownMessageType";
	const std::string kParseErrorStr = "ParseError";
	const std::string kUnknownErrorStr = "UnknownError";
}

const std::string &ProtobufCodecLite::errorCodeToString(ErrorCode errorCode)
{
	switch (errorCode)
	{
		case kNoError:
		 return kNoErrorStr;
		case kInvalidLength:
		 return kInvalidLengthStr;
		case kCheckSumError:
		 return kCheckSumErrorStr;
		case kInvalidNameLen:
		 return kInvalidNameLenStr;
		case kUnknownMessageType:
		 return kUnknownMessageTypeStr;
		case kParseError:
		 return kParseErrorStr;
		default:
		 return kUnknownErrorStr;
	}
}

void ProtobufCodecLite::defaultErrorCallback(const TcpConnectionPtr &conn,Buffer *buf,ErrorCode errorCode)
{
	LOG_ERROR << "ProtobufCodecLite::defaultErrorCallback - " << errorCodeToString(errorCode);
	if (conn && conn->connected())
	{
		conn->shutdown();
	}
}

int32_t ProtobufCodecLite::asInt32(const char* buf)
{
	int32_t be32 = 0;
	::memcpy(&be32, buf, sizeof(be32));
	return be32;
}

int32_t ProtobufCodecLite::checkSum(const void *buf,int len)
{
	return static_cast<int32_t>(::adler32(1, static_cast<const Bytef*>(buf),len));
}

bool ProtobufCodecLite::validateCheckSum(const char *buf,int len)
{
	// check sum
	int32_t expectedCheckSum = asInt32(buf + len - kChecksumLen);
	int32_t checksum = checkSum(buf, len - kChecksumLen);
	return checksum == expectedCheckSum;
}

ProtobufCodecLite::ErrorCode ProtobufCodecLite::parse(const char *buf,int len,::google::protobuf::Message *message)
{
	ErrorCode error = kNoError;

	if (validateCheckSum(buf,len))
	{
		if (memcmp(buf,tag.data(),tag.size()) == 0)
		{
			// parse from buffer
			const char *data = buf + tag.size();
			int32_t dataLen = len - kChecksumLen - static_cast<int>(tag.size());
			if (parseFromBuffer(StringPiece(data,dataLen),message))
			{
				error = kNoError;
			}
			else
			{
				error = kParseError;
			}
		}
		else
		{
			error = kUnknownMessageType;
		}
	}
	else
	{
		error = kCheckSumError;
	}

	return error;
}


