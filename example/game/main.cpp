#include <iostream>
#include <mutex>
#include <atomic>
#include "eventloop.h"


eventLoop loop;

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        fprintf(stderr, "Usage: <listen port> <net work thread num>\n");
        exit(-1);
    }

    std::thread t(std::bind(&eventLoop::run,&loop));

    auto server = std::make_shared<WrapTcpService>();
    auto listenThread = ListenThread::Create();

    listenThread->startListen(false, "0.0.0.0", atoi(argv[1]), [=](TcpSocket::PTR socket){
        socket->SocketNodelay();
        server->addSession(std::move(socket), [](const TCPSession::PTR& session){
            session->setDataCallback([](const TCPSession::PTR& session, const char* buffer, size_t len){
            	const char* parseStr = buffer;
				int totalProcLen = 0;
				size_t leftLen = len;
				auto HEAD_LEN = sizeof(uint32_t) + sizeof(uint32_t);

				while(true)
				{
					if (leftLen >= HEAD_LEN)
					{
						BasePacketReader rp(parseStr, leftLen);
						auto packetLen = rp.readUINT32();
						if(packetLen < leftLen - sizeof(uint32_t))
						{
							break;
						}

						std::shared_ptr<packet> p(new packet);
						p->id = rp.readUINT32();
						p->conn = session;
						p->data = (char*)zmalloc(packetLen - sizeof(uint32_t));
						memcpy(p->data,parseStr + rp.getPos(),packetLen - sizeof(uint32_t));
						loop.pushMsg(p);

						totalProcLen += packetLen;
						leftLen -= packetLen;
						//rp.skipAll();
					}
					else
					{
						break;
					}

				}


                return totalProcLen;
            });

            session->setDisConnectCallback([](const TCPSession::PTR& session){
            	loop.pushSession(session->getSocketID());
            });
        }, false, nullptr, 1024*1024);
    });

    server->startWorkThread(atoi(argv[2]));

    EventLoop mainLoop;
    while (true)
    {
        mainLoop.loop(10000);
    }
}



