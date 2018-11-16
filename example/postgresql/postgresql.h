#include "callback.h"
#include "buffer.h"
#include "sds.h"

/*
 * Option flags for PQcopyResult
 */
#define PG_COPYRES_ATTRS		  0x01
#define PG_COPYRES_TUPLES		  0x02	/* Implies PG_COPYRES_ATTRS */
#define PG_COPYRES_EVENTS		  0x04
#define PG_COPYRES_NOTICEHOOKS	  0x08
#define PG_PROTOCOL_MAJOR(v)	((v) >> 16)
#define PG_PROTOCOL_MINOR(v)	((v) & 0x0000ffff)
#define PG_PROTOCOL(m,n)	(((m) << 16) | (n))

/*
 * This macro lists the backend message types that could be "long" (more
 * than a couple of kilobytes).
 */
#define VALID_LONG_MESSAGE_TYPE(id) \
	((id) == 'T' || (id) == 'D' || (id) == 'd' || (id) == 'V' || \
	 (id) == 'E' || (id) == 'N' || (id) == 'A')

 /*
  * Although it is okay to add to these lists, values which become unused
  * should never be removed, nor should constants be redefined - that would
  * break compatibility with existing code.
  */

typedef enum
{
	CONNECTION_OK,
	CONNECTION_BAD,
	/* Non-blocking mode only below here */

	/*
	 * The existence of these should never be relied upon - they should only
	 * be used for user feedback or similar purposes.
	 */
	CONNECTION_STARTED,			/* Waiting for connection to be made.  */
	CONNECTION_MADE,			/* Connection OK; waiting to send.     */
	CONNECTION_AWAITING_RESPONSE,	/* Waiting for a response from the
									 * postmaster.        */
	CONNECTION_AUTH_OK,			/* Received authentication; waiting for
								 * backend startup. */
	CONNECTION_SETENV,			/* Negotiating environment. */
	CONNECTION_SSL_STARTUP,		/* Negotiating SSL. */
	CONNECTION_NEEDED,			/* Internal state: connect() needed */
	CONNECTION_CHECK_WRITABLE,	/* Check if we could make a writable
								 * connection. */
	CONNECTION_CONSUME			/* Wait for any pending message and consume
								 * them. */
} ConnStatusType;

typedef enum
{
	PGRES_POLLING_FAILED = 0,
	PGRES_POLLING_READING,		/* These two indicate that one may	  */
	PGRES_POLLING_WRITING,		/* use select before polling again.   */
	PGRES_POLLING_OK,
	PGRES_POLLING_ACTIVE		/* unused; keep for awhile for backwards
								 * compatibility */
} PostgresPollingStatusType;


typedef enum
{
	PGRES_EMPTY_QUERY = 0,		/* empty query string was executed */
	PGRES_COMMAND_OK,			/* a query command that doesn't return
								 * anything was executed properly by the
								 * backend */
	PGRES_TUPLES_OK,			/* a query command that returns tuples was
								 * executed properly by the backend, PGresult
								 * contains the result tuples */
	PGRES_COPY_OUT,				/* Copy Out data transfer in progress */
	PGRES_COPY_IN,				/* Copy In data transfer in progress */
	PGRES_BAD_RESPONSE,			/* an unexpected response was recv'd from the
								 * backend */
	PGRES_NONFATAL_ERROR,		/* notice or warning message */
	PGRES_FATAL_ERROR,			/* query failed */
	PGRES_COPY_BOTH,			/* Copy In/Out data transfer in progress */
	PGRES_SINGLE_TUPLE			/* single tuple from larger resultset */
} ExecStatusType;

typedef enum
{
	PQTRANS_IDLE,				/* connection idle */
	PQTRANS_ACTIVE,				/* command in progress */
	PQTRANS_INTRANS,			/* idle, within transaction block */
	PQTRANS_INERROR,			/* idle, within failed transaction */
	PQTRANS_UNKNOWN				/* cannot determine status */
} PGTransactionStatusType;

typedef enum
{
	PQERRORS_TERSE,				/* single-line error messages */
	PQERRORS_DEFAULT,			/* recommended style */
	PQERRORS_VERBOSE			/* all the facts, ma'am */
} PGVerbosity;

typedef enum
{
	PQSHOW_CONTEXT_NEVER,		/* never show CONTEXT field */
	PQSHOW_CONTEXT_ERRORS,		/* show CONTEXT for errors only (default) */
	PQSHOW_CONTEXT_ALWAYS		/* always show CONTEXT field */
} PGContextVisibility;

/* PGQueryClass tracks which query protocol we are now executing */
typedef enum
{
	PGQUERY_SIMPLE,				/* simple Query protocol (PQexec) */
	PGQUERY_EXTENDED,			/* full Extended protocol (PQexecParams) */
	PGQUERY_PREPARE,			/* Parse only (PQprepare) */
	PGQUERY_DESCRIBE			/* Describe Statement or Portal */
} PGQueryClass;

/* PGSetenvStatusType defines the state of the PQSetenv state machine */
/* (this is used only for 2.0-protocol connections) */
typedef enum
{
	SETENV_STATE_CLIENT_ENCODING_SEND,	/* About to send an Environment Option */
	SETENV_STATE_CLIENT_ENCODING_WAIT,	/* Waiting for above send to complete */
	SETENV_STATE_OPTION_SEND,	/* About to send an Environment Option */
	SETENV_STATE_OPTION_WAIT,	/* Waiting for above send to complete */
	SETENV_STATE_QUERY1_SEND,	/* About to send a status query */
	SETENV_STATE_QUERY1_WAIT,	/* Waiting for query to complete */
	SETENV_STATE_QUERY2_SEND,	/* About to send a status query */
	SETENV_STATE_QUERY2_WAIT,	/* Waiting for query to complete */
	SETENV_STATE_IDLE
} PGSetenvStatusType;

/* PGAsyncStatusType defines the state of the query-execution state machine */
typedef enum
{
	PGASYNC_IDLE,				/* nothing's happening, dude */
	PGASYNC_BUSY,				/* query in progress */
	PGASYNC_READY,				/* result ready for PQgetResult */
	PGASYNC_COPY_IN,			/* Copy In data transfer in progress */
	PGASYNC_COPY_OUT,			/* Copy Out data transfer in progress */
	PGASYNC_COPY_BOTH			/* Copy In/Out data transfer in progress */
} PGAsyncStatusType;

/*
 * PGPing - The ordering of this enum should not be altered because the
 * values are exposed externally via pg_isready.
 */

typedef enum
{
	PQPING_OK,					/* server is accepting connections */
	PQPING_REJECT,				/* server is alive but rejecting connections */
	PQPING_NO_RESPONSE,			/* could not establish connection */
	PQPING_NO_ATTEMPT			/* connection not attempted (bad params) */
} PGPing;

/* Typedef for the EnvironmentOptions[] array */
struct PQEnvironmentOption
{
	const char *envName;		/* name of an environment variable */
	const char *pgName;		/* name of corresponding SET variable */
};

struct PGnotify
{
	std::string relname;		/* notification condition name */
	int bePid;			/* process ID of notifying server process */
	std::string	 extra;			/* notification parameter */
	/* Fields below here are private to libpq; apps should not use 'em */
};

struct PGresAttDesc
{
	std::string name;			/* column name */
	size_t tableid;		/* source table, if known */
	int columnid;		/* source column, if known */
	int format;			/* format code for value (text/binary) */
	size_t typid;			/* type id */
	int typlen;			/* type size */
	int atttypmod;		/* type-specific modifier info */
};

/* Typedef for parameter-status list entries */
struct PGParameterStatus
{
	std::string name;			/* parameter name */
	std::string	value;			/* parameter value */
};

class PGresult
{
public:
	PGresult() {}
	~PGresult() {}
	int ntups;
	int numAttributes;
	std::vector<std::shared_ptr<PGresAttDesc>> attDescs;
	std::vector<std::vector<std::string>> tuples;	/* each PGresTuple is an array of  * PGresAttValue's */
	std::string errMsg;			/* error message, or NULL if no error */
	std::string	errQuery;		/* text of triggering query, if available */
	ExecStatusType status;
	std::string cmdStatus;
	int binary;			/* binary tuple values if binary == 1, */
private:
	PGresult(const PGresult&);
	void operator=(const PGresult&);
};

/*
 * PGconn stores all the state data associated with a single connection
 * to a backend.
 */
class PGconn
{
public:
	PGconn();
	~PGconn();

	char *pghost; /* the machine on which the server is running,
								 * or a path to a UNIX-domain socket, or a
								 * comma-separated list of machines and/or
								 * paths; if NULL, use DEFAULT_PGSOCKET_DIR */
	char *pghostaddr; /* the numeric IP address of the machine on
								 * which the server is running, or a
								 * comma-separated list of same.  Takes
								 * precedence over pghost. */
	int  pgport; /* the server's communication port number, or
								 * a comma-separated list of ports */
								 /* Callback procedures for notice message processing */

	char *pguser;
	char *dbName;

	int bePid;
	int beKey;

	/* Status indicators */
	ConnStatusType status;
	PGAsyncStatusType asyncStatus;
	PGTransactionStatusType xactStatus; /* never changes to ACTIVE */
	PGQueryClass queryclass;

	PGSetenvStatusType setenvState;	/* for 2.0 protocol only */
	bool optionsValid;	/* true if OK to attempt connection */
	bool nonblocking;	/* whether this connection is using nonblock  sending semantics */
	bool stdStrings;	/* standard_conforming_strings */
	PGVerbosity verbosity;		/* error/notice message verbosity */
	PGContextVisibility showContext;	/* whether to show CONTEXT field */
	std::list<std::shared_ptr<PGnotify>> notify;
	int sockfd;	/* FD for socket, PGINVALID_SOCKET if  * unconnected */
	uint32_t pversion;
	Buffer inBuffer;	/* currently allocated buffer */
	int inCursor;		/* next byte to tentatively consume */
	int inStart;
	Buffer outBuffer;	/* currently allocated buffer */
	/* Row processor interface workspace */
	std::vector<std::string > rowBuffer;		/* array for passing values to rowProcessor */
	/* Buffer for current error message */
	std::string errorMessage;	/* expansible string */
	/* Buffer for receiving various parts of messages */
	std::string workBuffer; /* expansible string */
	std::shared_ptr<PGresult> result;			/* result being constructed */
	std::list<std::shared_ptr<PGParameterStatus>> pstatus; /* ParameterStatus data */

private:
	PGconn(const PGconn&);
	void operator=(const PGconn&);
};

std::shared_ptr<PGconn> PQconnectStart(const char *conninfo);
char *pqBuildStartupPacket3(const std::shared_ptr<PGconn> &conn, int *packetlen,
					  const PQEnvironmentOption *options);
int buildStartupPacket(const std::shared_ptr<PGconn> &conn, char *packet, const PQEnvironmentOption *options);
std::shared_ptr<PGresult> PQexec(const std::shared_ptr<PGconn> &conn, const char *query);
bool PQexecStart(const std::shared_ptr<PGconn> &conn);
std::shared_ptr<PGresult> PQgetResult(const std::shared_ptr<PGconn> &conn);
int pqGetc(char *result, const std::shared_ptr<PGconn> &conn);
int pqGetInt(int *result, size_t bytes, const std::shared_ptr<PGconn> &conn);
int getNotify(const std::shared_ptr<PGconn> &conn);
int pqGets(std::string &buff, const std::shared_ptr<PGconn> &conn);
void pqParseInput3(const std::shared_ptr<PGconn> &conn);
int getParameterStatus(const std::shared_ptr<PGconn> &conn);
void pqSaveParameterStatus(const std::shared_ptr<PGconn> &conn, const char *name, const char *value);
std::shared_ptr<PGresult> PQmakeEmptyPGresult(const std::shared_ptr<PGconn> &conn, ExecStatusType status);
int getReadyForQuery(const std::shared_ptr<PGconn> &conn);
int getRowDescriptions(const std::shared_ptr<PGconn> &conn, int msgLength);
void pqSaveErrorResult(const std::shared_ptr<PGconn> &conn);
int getAnotherTuple(PGconn *conn, int msgLength);
int pqSkipnchar(size_t len, const std::shared_ptr<PGconn> &conn);
int pqRowProcessor(const std::shared_ptr<PGconn> &conn, std::string &errmsgp);
bool pqAddTuple(std::shared_ptr<PGresult> res,std::vector<std::string> tup, std::string &errmsgp);
int PQputCopyEnd(const std::shared_ptr<PGconn> &conn, std::string &errormsg);
int pqPutMsgStart(char msgType, bool force, const std::shared_ptr<PGconn> &conn);
int pqPuts(const  char *s, const std::shared_ptr<PGconn> &conn);
int pqPutMsgEnd(const std::shared_ptr<PGconn> &conn);
std::shared_ptr<PGresult> PQexecFinish(const std::shared_ptr<PGconn> &conn);
int pqReadData(const std::shared_ptr<PGconn> &conn);
std::shared_ptr<PGresult> getCopyResult(const std::shared_ptr<PGconn> &conn, ExecStatusType copytype);
