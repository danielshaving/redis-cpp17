#include "all.h"
#include "hiredis.h"

std::map <int16_t, RedisContextPtr> redisContexts;

void cluster(RedisReplyPtr &reply, const std::string &str) {
    if (reply->type == REDIS_REPLY_ERROR &&
        (!strncmp(reply->str, "MOVED", 5) || (!strncmp(reply->str, "ASK", 3)))) {
        char *p = reply->str, *s;
        int32_t slot;
        s = strchr(p, ' ');
        p = strchr(s + 1, ' ');
        *p = '\0';
        slot = atoi(s + 1);
        s = strrchr(p + 1, ':');
        *s = '\0';

        const char *ip = p + 1;
        int16_t port = atoi(s + 1);
        //printf("-> Redirected to slot %d located at %s %d\n", slot , ip, port);

        RedisContextPtr c;
        auto it = redisContexts.find(port);
        if (it == redisContexts.end()) {
            RedisContextPtr c1 = redisConnect(ip, port);
            if (c1 == nullptr || c1->err) {
                if (c1) {
                    printf("Redis %s:%d Connection error: %s\n", ip, port, c1->errstr.c_str());
                } else {
                    printf("Connection error: can't allocate redis context\n");
                }
                exit(1);
            }
            redisContexts[port] = c1;
            c = c1;
        } else {
            c = it->second;
        }

        if (!strncmp(reply->str, "MOVED", 5)) {
            RedisReplyPtr reply1 = c->redisCommand(str.c_str());
            assert(reply1 != nullptr);
            reply = reply1;
        } else if (!strncmp(reply->str, "ASK", 3)) {
            RedisReplyPtr reply1 = c->redisCommand("*1\r\n$6\r\nASKING\r\n");
            assert(reply1 != nullptr);
            assert(reply1->type == REDIS_REPLY_STATUS);
            reply1 = c->redisCommand(str.c_str());
            assert(reply1 != nullptr);
            reply = reply1;
        }
    } else {
        assert(false);
    }
}

int main() {
#ifdef _WIN64
    WSADATA wsaData;
    int32_t iRet = WSAStartup(MAKEWORD(2, 2), &wsaData);
    assert(iRet == 0);
#else
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
#endif

    const char *ip = "127.0.0.1";
    int16_t port = 6379;
    int16_t port1 = 7000;

    RedisContextPtr c = redisConnect(ip, port);
    if (c == nullptr || c->err) {
        if (c) {
            printf("Redis %s:%d Connection error: %s\n", ip, port, c->errstr.c_str());
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    RedisContextPtr c1 = redisConnect(ip, port1);
    if (c1 == nullptr || c1->err) {
        if (c1) {
            printf("Redis %s:%d Connection error: %s\n", ip, port1, c1->errstr.c_str());
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    redisContexts[port1] = c1;
    RedisReplyPtr reply = c->redisCommand("keys *");
    assert(reply != nullptr);
    assert(reply->type == REDIS_REPLY_ARRAY);
    for (auto &it : reply->element) {
        std::string str = "type ";
        str += std::string(it->str, sdslen(it->str));
        RedisReplyPtr reply1 = c->redisCommand(str.c_str(), str.size());
        assert(reply1 != nullptr);
        assert(reply1->type == REDIS_REPLY_STATUS);

        std::string str1 = "ttl ";
        str1 += std::string(it->str, sdslen(it->str));
        RedisReplyPtr reply2 = c->redisCommand(str1.c_str(), str1.size());
        assert(reply2 != nullptr);
        assert(reply2->type == REDIS_REPLY_INTEGER);

        if (memcmp(reply1->str, "string", sdslen(reply1->str)) == 0) {
            std::string str2 = "get ";
            str2 += std::string(it->str, sdslen(it->str));
            RedisReplyPtr reply3 = c->redisCommand(str2.c_str(), str2.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_STRING) {
                cluster(reply3, str2);
            }

            assert(reply3->type == REDIS_REPLY_STRING);
            std::string str3 = "set ";
            str3 += std::string(it->str, sdslen(it->str));
            str3 += " ";
            str3 += std::string(reply3->str, sdslen(reply3->str));

            if (reply2->integer > 0) {
                str3 += " ";
                str3 += "ex ";
                str3 += std::to_string(reply2->integer);
            }

            //printf("%s\n", str3.c_str());
            RedisReplyPtr reply4 = c1->redisCommand(str3.c_str(), str3.size());
            assert(reply4 != nullptr);
            if (reply4->type != REDIS_REPLY_STATUS) {
                cluster(reply4, str3);
            }
            assert(reply4->type == REDIS_REPLY_STATUS);
        } else if (memcmp(reply1->str, "hash", sdslen(reply1->str)) == 0) {
            std::string str2 = "hgetall ";
            str2 += std::string(it->str, sdslen(it->str));
            RedisReplyPtr reply3 = c->redisCommand(str2.c_str(), str2.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_ARRAY) {
                cluster(reply3, str2);
            }

            assert(reply3->type == REDIS_REPLY_ARRAY);
            for (int i = 0; i < reply3->element.size(); i += 2) {
                std::string str3 = "hset ";
                str3 += std::string(it->str, sdslen(it->str));
                str3 += " ";
                str3 += std::string(reply3->element[i]->str, sdslen(reply3->element[i]->str));
                str3 += " ";
                str3 += std::string(reply3->element[i + 1]->str, sdslen(reply3->element[i + 1]->str));

                //printf("%s\n", str3.c_str());
                RedisReplyPtr reply4 = c1->redisCommand(str3.c_str(), str3.size());
                assert(reply4 != nullptr);
                if (reply4->type != REDIS_REPLY_INTEGER) {
                    cluster(reply4, str3);
                }
                assert(reply4->type == REDIS_REPLY_INTEGER);
            }
        } else if (memcmp(reply1->str, "list", sdslen(reply1->str)) == 0) {
            std::string str2 = "lrange ";
            str2 += std::string(it->str, sdslen(it->str));
            str2 += " 0 -1";

            RedisReplyPtr reply3 = c->redisCommand(str2.c_str(), str2.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_ARRAY) {
                cluster(reply3, str2);
            }

            assert(reply3->type == REDIS_REPLY_ARRAY);
            for (int i = 0; i < reply3->element.size(); i++) {
                std::string str3 = "lpush ";
                str3 += std::string(it->str, sdslen(it->str));
                str3 += " ";
                str3 += std::string(reply3->element[i]->str, sdslen(reply3->element[i]->str));

                //printf("%s\n", str3.c_str());
                RedisReplyPtr reply4 = c1->redisCommand(str3.c_str(), str3.size());
                assert(reply4 != nullptr);
                if (reply4->type != REDIS_REPLY_INTEGER) {
                    cluster(reply4, str3);
                }
                assert(reply4->type == REDIS_REPLY_INTEGER);
            }
        } else if (memcmp(reply1->str, "set", sdslen(reply1->str)) == 0) {
            std::string str2 = "smembers  ";
            str2 += std::string(it->str, sdslen(it->str));

            RedisReplyPtr reply3 = c->redisCommand(str2.c_str(), str2.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_ARRAY) {
                cluster(reply3, str2);
            }

            assert(reply3->type == REDIS_REPLY_ARRAY);
            for (int i = 0; i < reply3->element.size(); i++) {
                std::string str3 = "sadd ";
                str3 += std::string(it->str, sdslen(it->str));
                str3 += " ";
                str3 += std::string(reply3->element[i]->str, sdslen(reply3->element[i]->str));

                //printf("%s\n", str3.c_str());
                RedisReplyPtr reply4 = c1->redisCommand(str3.c_str(), str3.size());
                assert(reply4 != nullptr);
                if (reply4->type != REDIS_REPLY_INTEGER) {
                    cluster(reply4, str3);
                }
                assert(reply4->type == REDIS_REPLY_INTEGER);
            }
        } else if (memcmp(reply1->str, "zset", sdslen(reply1->str)) == 0) {
            std::string str2 = "zrange ";
            str2 += std::string(it->str, sdslen(it->str));
            str2 += " 0 -1 withscores";

            RedisReplyPtr reply3 = c->redisCommand(str2.c_str(), str2.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_ARRAY) {
                cluster(reply3, str2);
            }

            assert(reply3->type == REDIS_REPLY_ARRAY);
            for (int i = 0; i < reply3->element.size(); i += 2) {
                std::string str3 = "zadd ";
                str3 += std::string(it->str, sdslen(it->str));
                str3 += " ";
                str3 += std::string(reply3->element[i]->str, sdslen(reply3->element[i]->str));
                str3 += " ";
                str3 += std::string(reply3->element[i + 1]->str, sdslen(reply3->element[i + 1]->str));

                //printf("%s\n", str3.c_str());
                RedisReplyPtr reply4 = c1->redisCommand(str3.c_str(), str3.size());
                assert(reply4 != nullptr);
                if (reply4->type != REDIS_REPLY_INTEGER) {
                    cluster(reply4, str3);
                }
                assert(reply4->type == REDIS_REPLY_INTEGER);
            }
        } else {
            assert(false);
        }
    }

    RedisReplyPtr db1 = c->redisCommand("dbsize");
    assert(db1 != nullptr);
    assert(db1->type == REDIS_REPLY_INTEGER);
    int64_t dbsize = 0;
    for (auto &it : redisContexts) {
        RedisReplyPtr db2 = it.second->redisCommand("dbsize");
        assert(db2 != nullptr);
        assert(db2->type == REDIS_REPLY_INTEGER);
        dbsize += db2->integer;
    }

    assert(db1->integer == dbsize);

#ifndef DEBUG
    reply = c->redisCommand("keys *");
    assert(reply != nullptr);
    assert(reply->type == REDIS_REPLY_ARRAY);
    for (auto &it : reply->element) {
        std::string str = "type ";
        str += std::string(it->str, sdslen(it->str));
        RedisReplyPtr reply1 = c->redisCommand(str.c_str(), str.size());
        assert(reply1 != nullptr);
        assert(reply1->type == REDIS_REPLY_STATUS);

        if (memcmp(reply1->str, "string", sdslen(reply1->str)) == 0) {
            std::string str1 = "get ";
            str1 += std::string(it->str, sdslen(it->str));
            RedisReplyPtr reply2 = c->redisCommand(str1.c_str(), str1.size());
            assert(reply2 != nullptr);
            if (reply2->type != REDIS_REPLY_STRING) {
                cluster(reply2, str1);
            }

            assert(reply2->type == REDIS_REPLY_STRING);
            RedisReplyPtr reply3 = c1->redisCommand(str1.c_str(), str1.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_STRING) {
                cluster(reply3, str1);
            }

            assert(reply3->type == REDIS_REPLY_STRING);
            assert(sdscmp(reply2->str, reply3->str) == 0);
        } else if (memcmp(reply1->str, "hash", sdslen(reply1->str)) == 0) {
            std::string str1 = "hgetall ";
            str1 += std::string(it->str, sdslen(it->str));
            RedisReplyPtr reply2 = c->redisCommand(str1.c_str(), str1.size());
            assert(reply2 != nullptr);
            if (reply2->type != REDIS_REPLY_ARRAY) {
                cluster(reply2, str1);
            }

            assert(reply2->type == REDIS_REPLY_ARRAY);
            RedisReplyPtr reply3 = c1->redisCommand(str1.c_str(), str1.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_ARRAY) {
                cluster(reply3, str1);
            }

            assert(reply3->type == REDIS_REPLY_ARRAY);
            assert(reply2->element.size() == reply3->element.size());

            for (int i = 0; i < reply2->element.size(); i++) {
                assert(sdscmp(reply2->element[i]->str, reply3->element[i]->str) == 0);
            }
        } else if (memcmp(reply1->str, "list", sdslen(reply1->str)) == 0) {
            std::string str1 = "lrange ";
            str1 += std::string(it->str, sdslen(it->str));
            str1 += " 0 -1";

            RedisReplyPtr reply2 = c->redisCommand(str1.c_str(), str1.size());
            assert(reply2 != nullptr);
            if (reply2->type != REDIS_REPLY_ARRAY) {
                cluster(reply2, str1);
            }

            assert(reply2->type == REDIS_REPLY_ARRAY);
            RedisReplyPtr reply3 = c1->redisCommand(str1.c_str(), str1.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_ARRAY) {
                cluster(reply3, str1);
            }

            assert(reply3->type == REDIS_REPLY_ARRAY);
            assert(reply2->element.size() == reply3->element.size());

            for (int i = 0; i < reply2->element.size(); i++) {
                assert(sdscmp(reply2->element[i]->str, reply3->element[i]->str) == 0);
            }
        } else if (memcmp(reply1->str, "set", sdslen(reply1->str)) == 0) {
            std::string str1 = "smembers  ";
            str1 += std::string(it->str, sdslen(it->str));
            RedisReplyPtr reply2 = c->redisCommand(str1.c_str(), str1.size());
            assert(reply2 != nullptr);
            if (reply2->type != REDIS_REPLY_ARRAY) {
                cluster(reply2, str1);
            }

            assert(reply2->type == REDIS_REPLY_ARRAY);
            RedisReplyPtr reply3 = c1->redisCommand(str1.c_str(), str1.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_ARRAY) {
                cluster(reply3, str1);
            }

            assert(reply3->type == REDIS_REPLY_ARRAY);
            assert(reply2->element.size() == reply3->element.size());

            for (int i = 0; i < reply2->element.size(); i++) {
                assert(sdscmp(reply2->element[i]->str, reply3->element[i]->str) == 0);
            }
        } else if (memcmp(reply1->str, "zset", sdslen(reply1->str)) == 0) {
            std::string str1 = "zrange ";
            str1 += std::string(it->str, sdslen(it->str));
            str1 += " 0 -1 withscores";

            RedisReplyPtr reply2 = c->redisCommand(str1.c_str(), str1.size());
            assert(reply2 != nullptr);
            if (reply2->type != REDIS_REPLY_ARRAY) {
                cluster(reply2, str1);
            }

            assert(reply2->type == REDIS_REPLY_ARRAY);
            RedisReplyPtr reply3 = c1->redisCommand(str1.c_str(), str1.size());
            assert(reply3 != nullptr);
            if (reply3->type != REDIS_REPLY_ARRAY) {
                cluster(reply3, str1);
            }

            assert(reply3->type == REDIS_REPLY_ARRAY);
            assert(reply2->element.size() == reply3->element.size());

            for (int i = 0; i < reply2->element.size(); i++) {
                assert(sdscmp(reply2->element[i]->str, reply3->element[i]->str) == 0);
            }
        } else {
            assert(false);
        }
    }
#endif
    return 0;
}
