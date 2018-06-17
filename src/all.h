//
// Created by zhanghao on 2018/6/17.
//
#pragma once
#include <inttypes.h>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <list>
#include <map>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <functional>
#include <algorithm>
#include <memory>
#include <condition_variable>
#include <thread>
#include <sys/syscall.h>
#include <sys/types.h>
#include <signal.h>
#include <string>
#include <iosfwd>    // for ostream forward-declaration
#include <string>
#include <sys/socket.h>
#include <set>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/uio.h>
#include <array>
#include <utility>
#include <limits.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <stdio.h>
#include <atomic>
#include <stdarg.h>
#include <sys/resource.h>
#include <limits.h>
#include <poll.h>
#include <netdb.h>
#include <sys/wait.h>
#include <any>
#include <arpa/inet.h>
#include <netinet/in.h>

#ifdef __APPLE__
#include <sys/event.h>
#include <sys/un.h>
#endif

#ifdef __linux__
#include <sys/sendfile.h>
#include <sys/epoll.h>
#include <linux/tcp.h>
#include <sys/timerfd.h>
#include <sys/eventfd.h>
#include <endian.h>
#include <sys/un.h>
#endif

#define REDIS_CONNECT_RETRIES  10


/* Flag specific to the async API which means that the context should be clean
 * up as soon as possible. */
#define REDIS_FREEING 0x8

/* Flag that is set when an async callback is executed. */
#define REDIS_IN_CALLBACK 0x10

/* Flag that is set when the async context has one or more subscriptions. */
#define REDIS_SUBSCRIBED 0x20

/* Flag that is set when monitor mode is active */
#define REDIS_MONITORING 0x40

/* Flag that is set when we should set SO_REUSEADDR before calling bind() */
#define REDIS_REUSEADDR 0x80



#define REDIS_ENCODING_EMBSTR_SIZE_LIMIT 39
/* Client request types */
#define REDIS_REQ_INLINE 1
#define REDIS_REQ_MULTIBULK 2
/* Error codes */
#define REDIS_OK                1
#define REDIS_ERR               0
#define REDIS_INLINE_MAX_SIZE   (4096 * 64 *10 * 10) /* Max size of inline reads */
#define REDIS_LRU_BITS 24
#define REDIS_MBULK_BIG_ARG     (4096 * 11 * 10 * 10)
#define REDIS_NULL -1
#define REDIS_STRING 0
#define REDIS_LIST 1
#define REDIS_SET 2
#define REDIS_ZSET 3
#define REDIS_HASH 4
#define REDIS_EXPIRE 5

/* Object types */
#define OBJ_STRING 0
#define OBJ_LIST 1
#define OBJ_SET 2
#define OBJ_ZSET 3
#define OBJ_HASH 4
#define OBJ_EXPIRE 5

#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)     /* Set if key not exists. */
#define OBJ_SET_XX (1<<1)     /* Set if key exists. */
#define OBJ_SET_EX (1<<2)     /* Set if time in seconds is given */
#define OBJ_SET_PX (1<<3)     /* Set if time in ms in given */


/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1


#define OBJ_ENCODING_RAW 0     /* Raw representation */
#define OBJ_ENCODING_INT 1     /* Encoded as integer */
#define OBJ_ENCODING_HT 2      /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3  /* Encoded as zipmap */
#define OBJ_ENCODING_LINKEDLIST 4 /* Encoded as regular linked list */
#define OBJ_ENCODING_ZIPLIST 5 /* Encoded as ziplist */
#define OBJ_ENCODING_INTSET 6  /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8  /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9 /* Encoded as linked list of ziplists */



/* Static server configuration */
#define REDIS_COMMAND_LENGTH 15
#define REDIS_DEFAULT_HZ        10      /* Time interrupt calls/sec. */
#define REDIS_MIN_HZ            1
#define REDIS_MAX_HZ            500
#define REDIS_SERVERPORT        6379    /* TCP port */
#define REDIS_TCP_BACKLOG       511     /* TCP listen backlog */
#define REDIS_MAXIDLETIME       0       /* default client timeout: infinite */
#define REDIS_DEFAULT_DBNUM     16
#define REDIS_CONFIGLINE_MAX    1024
#define REDIS_DBCRON_DBS_PER_CALL 16
#define REDIS_MAX_WRITE_PER_EVENT (1024*64)
#define REDIS_SHARED_SELECT_CMDS 10
#define REDIS_SHARED_INTEGERS 100000
#define REDIS_SHARED_BULKHDR_LEN 32
#define REDIS_MAX_LOGMSG_LEN    1024 /* Default maximum lengthgth of syslog messages */
#define REDIS_AOF_REWRITE_PERC  100
#define REDIS_AOF_REWRITE_MIN_SIZE (64*1024*1024)
#define REDIS_AOF_REWRITE_ITEMS_PER_CMD 64
#define REDIS_SLOWLOG_LOG_SLOWER_THAN 10000
#define REDIS_SLOWLOG_MAX_LENGTH 128
#define REDIS_MAX_CLIENTS 10000
#define REDIS_AUTHPASS_MAX_LENGTH 512
#define REDIS_DEFAULT_SLAVE_PRIORITY 100
#define REDIS_REPL_TIMEOUT 60
#define REDIS_REPL_PING_SLAVE_PERIOD 10
#define REDIS_RUN_ID_SIZE 40
#define REDIS_OPS_SEC_SAMPLES 16
#define REDIS_DEFAULT_REPL_BACKLOG_SIZE (1024*1024)    /* 1mb */
#define REDIS_DEFAULT_REPL_BACKLOG_TIME_LIMIT (60*60)  /* 1 hour */
#define REDIS_REPL_BACKLOG_MIN_SIZE (1024*16)          /* 16k */
#define REDIS_BGSAVE_RETRY_DELAY 5 /* Wait a few secs before trying again. */
#define REDIS_DEFAULT_PID_FILE "/var/run/redis.pid"
#define REDIS_DEFAULT_SYSLOG_IDENT "redis"
#define REDIS_DEFAULT_CLUSTER_CONFIG_FILE "nodes.conf"
#define REDIS_DEFAULT_DAEMONIZE 0
#define REDIS_DEFAULT_UNIX_SOCKET_PERM 0
#define REDIS_DEFAULT_TCP_KEEPALIVE 0
#define REDIS_DEFAULT_LOGFILE ""
#define REDIS_DEFAULT_SYSLOG_ENABLED 0
#define REDIS_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR 1
#define REDIS_DEFAULT_RDB_COMPRESSION 1
#define REDIS_DEFAULT_RDB_CHECKSUM 1
#define REDIS_DEFAULT_RDB_FILENGTHAME "dump.rdb"
#define REDIS_DEFAULT_SLAVE_SERVE_STALE_DATA 1
#define REDIS_DEFAULT_SLAVE_READ_ONLY 1
#define REDIS_DEFAULT_REPL_DISABLE_TCP_NODELAY 0
#define REDIS_DEFAULT_MAXMEMORY 0
#define REDIS_DEFAULT_MAXMEMORY_SAMPLES 5
#define REDIS_DEFAULT_AOF_FILENGTHAME "appendonly.aof"
#define REDIS_DEFAULT_AOF_NO_FSYNC_ON_REWRITE 0
#define REDIS_DEFAULT_ACTIVE_REHASHING 1
#define REDIS_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC 1
#define REDIS_DEFAULT_MIN_SLAVES_TO_WRITE 0
#define REDIS_DEFAULT_MIN_SLAVES_MAX_LAG 10
#define REDIS_IP_STR_LENGTH INET6_ADDRSTRLENGTH
#define REDIS_PEER_ID_LENGTH (REDIS_IP_STR_LENGTH+32) /* Must be enough for ip:port */
#define REDIS_BINDADDR_MAX 16
#define REDIS_MIN_RESERVED_FDS 32
#define REDIS_ENCODING_RAW 0     /* Raw representation */
#define REDIS_ENCODING_INT 1     /* Encoded as integer */
#define REDIS_ENCODING_HT 2      /* Encoded as hash table */
#define REDIS_ENCODING_ZIPMAP 3  /* Encoded as zipmap */
#define REDIS_ENCODING_LINKEDLIST 4 /* Encoded as regular linked list */
#define REDIS_ENCODING_ZIPLIST 5 /* Encoded as ziplist */
#define REDIS_ENCODING_INTSET 6  /* Encoded as intset */
#define REDIS_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define REDIS_ENCODING_EMBSTR 8  /* Embedded sds string encoding */

/* The current RDB version. When the format changes in a way that is no longer
 * backward compatible this number gets incremented. */
#define REDIS_RDB_VERSION 9

/* Defines related to the dump file format. To store 32 bits lengthgths for short
 * keys requires a lot of space, so we check the most significant 2 bits of
 * the first byte to interpreter the lengthgth:
 *
 * 00|000000 => if the two MSB are 00 the length is the 6 bits of this byte
 * 01|000000 00000000 =>  01, the length is 14 byes, 6 bits + 8 bits of next byte
 * 10|000000 [32 bit integer] => if it's 01, a full 32 bit length will follow
 * 11|000000 this means: specially encoded object will follow. The six bits
 *           number specify the kind of object that follows.
 *           See the REDIS_RDB_ENC_* defines.
 *
 * lengthgths up to 63 are stored using a single byte, most DB keys, and may
 * values, will fit inside. */
#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2
#define REDIS_RDB_ENCVAL 3
#define REDIS_RDB_LENERR UINT_MAX

/* When a lengthgth of a string object stored on disk has the first two bits
 * set, the remaining two bits specify a special encoding for the object
 * accordingly to the following defines: */
#define REDIS_RDB_ENC_INT8 0        /* 8 bit signed integer */
#define REDIS_RDB_ENC_INT16 1       /* 16 bit signed integer */
#define REDIS_RDB_ENC_INT32 2       /* 32 bit signed integer */
#define REDIS_RDB_ENC_LZF 3         /* string compressed with FASTLZ */

/* Dup object types to RDB object types. Only reason is readability (are we
 * dealing with RDB types or with in-memory object types?). */
#define REDIS_RDB_TYPE_STRING 0
#define REDIS_RDB_TYPE_LIST   1
#define REDIS_RDB_TYPE_SET    2
#define REDIS_RDB_TYPE_ZSET   3
#define REDIS_RDB_TYPE_HASH   4
#define REDIS_RDB_TYPE_EXPIRE  5

/* Object types for encoded objects. */
#define REDIS_RDB_TYPE_HASH_ZIPMAP    9
#define REDIS_RDB_TYPE_LIST_ZIPLIST  10
#define REDIS_RDB_TYPE_SET_INTSET    11
#define REDIS_RDB_TYPE_ZSET_ZIPLIST  12
#define REDIS_RDB_TYPE_HASH_ZIPLIST  13

/* Test if a type is an object type. */
#define rdbIsObjectType(t) ((t >= 0 && t <= 4) || (t >= 9 && t <= 13))

/* Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
#define REDIS_RDB_OPCODE_SET        250
#define REDIS_RDB_OPCODE_HSET       251

#define REDIS_RDB_OPCODE_EXPIRETIME_MS 252
#define REDIS_RDB_OPCODE_EXPIRETIME 253
#define REDIS_RDB_OPCODE_SELECTDB   254
#define REDIS_RDB_OPCODE_EOF        255

#define REDIS_RDB_STRING 100
#define REDIS_RDB_HSET 101
#define REDIS_RDB_SET 102
#define REDIS_RDB_SORT_SET 103
#define REDIS_RDB_LIST 104
#define REDIS_RDB_SORTSET 105

#define REDIS_EXPIRE_TIME 109

#define RDB_6BITLEN 0
#define RDB_14BITLEN 1
#define RDB_32BITLEN 0x80
#define RDB_64BITLEN 0x81
#define RDB_ENCVAL 3
#define RDB_LENERR UINT64_MAX


#define RDB_OPCODE_EOF 110

/* Protocol and I/O related defines */
#define PROTO_MAX_QUERYBUF_LENGTH  (1024*1024*1024) /* 1GB max query buffer. */
#define PROTO_IOBUF_LENGTH         (1024*64)  /* Generic I/O buffer size */
#define PROTO_REPLY_CHUNK_BYTES (64*1024) /* 64k output buffer */
#define PROTO_INLINE_MAX_SIZE   (1024*64 * 64) /* Max size of inline reads */
#define PROTO_MBULK_BIG_ARG     (1024*32)
#define LONG_STR_SIZE      21          /* Bytes needed for long -> str + '\0' */
#define AOF_AUTOSYNC_BYTES (1024*1024*32) /* fdatasync every 32MB */

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

#define REPLI_TIME_OUT	60

/* Connection type can be blocking or non-blocking and is set in the
 * least significant bit of the flags field in redisContext. */
#define REDIS_BLOCK 0x1

/* Connection may be disconnected before being free'd. The second bit
 * in the flags field is set when the context is connected. */
#define REDIS_CONNECTED 0x2

/* The async API might try to disconnect cleanly and flush the output
 * buffer and read all subsequent replies before disconnecting.
 * This flag means no new commands can come in and the connection
 * should be terminated once all replies have been read. */
#define REDIS_DISCONNECTING 0x4

/* Flag specific to the async API which means that the context should be clean
 * up as soon as possible. */
#define REDIS_FREEING 0x8

/* Flag that is set when an async callback is executed. */
#define REDIS_IN_CALLBACK 0x10

/* Flag that is set when the async context has one or more subscriptions. */
#define REDIS_SUBSCRIBED 0x20

/* Flag that is set when monitor mode is active */
#define REDIS_MONITORING 0x40


/* When an error occurs, the err flag in a context is set to hold the type of
 * error that occured. REDIS_ERR_IO means there was an I/O error and you
 * should use the "errno" variable to find out what is wrong.
 * For other values, the "errstr" field will hold a description. */
#define REDIS_ERR_IO 1 /* Error in read or write */
#define REDIS_ERR_EOF 3 /* End of file */
#define REDIS_ERR_PROTOCOL 4 /* Protocol error */
#define REDIS_ERR_OOM 5 /* Out of memory */
#define REDIS_ERR_OTHER 2 /* Everything else... */


#define REDIS_SLAVE_SYNC_SIZE  65536 
#define REDIS_RECONNECT_COUNT 10

#define CLUSTER_SLOTS 16384
#define CLUSTER_OK 0          /* Everything looks ok */
#define CLUSTER_FAIL 1        /* The cluster can't work */
#define CLUSTER_NAMELEN 40    /* sha1 hex lengthgth */
#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */


#define NET_IP_STR_LEN 46

 /* Redirection errors returned by getNodeByQuery(). */
#define CLUSTER_REDIR_NONE 0          /* Node can serve the request. */
#define CLUSTER_REDIR_CROSS_SLOT 1    /* -CROSSSLOT request. */
#define CLUSTER_REDIR_UNSTABLE 2      /* -TRYAGAIN redirection required */
#define CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */
#define CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */
#define CLUSTER_REDIR_DOWN_STATE 5    /* -CLUSTERDOWN, global state. */
#define CLUSTER_REDIR_DOWN_UNBOUND 6  /* -CLUSTERDOWN, unbound slot. */

#define CLUSTER_SYNC 0
#define CLUSTER_SYNCING  1
#define CLUSTER_SYNCED 2

#define CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN 10000
#define CONFIG_DEFAULT_SLOWLOG_MAX_LEN 128
#define CONFIG_DEFAULT_MAX_CLIENTS 10000
#define CONFIG_AUTHPASS_MAX_LEN 512
#define CONFIG_DEFAULT_SLAVE_PRIORITY 100
#define CONFIG_DEFAULT_REPL_TIMEOUT 60
#define CONFIG_DEFAULT_REPL_PING_SLAVE_PERIOD 10
#define CONFIG_RUN_ID_SIZE 40


#define OBJ_ENCODING_RAW 0     /* Raw representation */
#define OBJ_ENCODING_INT 1     /* Encoded as integer */
#define OBJ_ENCODING_HT 2      /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3  /* Encoded as zipmap */
#define OBJ_ENCODING_LINKEDLIST 4 /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5 /* Encoded as ziplist */
#define OBJ_ENCODING_INTSET 6  /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8  /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9 /* Encoded as linked list of ziplists */
#define OBJ_ENCODING_STREAM 10 /* Encoded as a radix tree of listpacks */

#define RDB_SAVE_NONE 0
#define RDB_SAVE_AOF_PREAMBLE (1<<0)


#define LONG_STR_SIZE      21          /* Bytes needed for long -> str + '\0' */

#define RDB_OPCODE_MODULE_AUX 247   /* Module auxiliary data. */
#define RDB_OPCODE_IDLE       248   /* LRU idle time. */
#define RDB_OPCODE_FREQ       249   /* LFU frequency. */
#define RDB_OPCODE_AUX        250   /* RDB aux field. */
#define RDB_OPCODE_RESIZEDB   251   /* Hash table resize hint. */
#define RDB_OPCODE_EXPIRETIME_MS 252    /* Expire time in milliseconds. */
#define RDB_OPCODE_EXPIRETIME 253       /* Old expire time in seconds. */
#define RDB_OPCODE_SELECTDB   254   /* DB number of the following keys. */
#define RDB_OPCODE_EOF        255   /* End of the RDB file. */

#define sdsEncodedObject(objptr) (objptr->encoding == REDIS_ENCODING_RAW || objptr->encoding == REDIS_ENCODING_EMBSTR)
#define UNUSED(V) ((void) V)

#define SDS_MAX_PREALLOC (1024*1024)
