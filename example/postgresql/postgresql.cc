#include "postgresql.h"
#include "sds.h"

PGconn::PGconn()
:status(CONNECTION_BAD),
 asyncStatus(PGASYNC_IDLE),
 xactStatus(PQTRANS_IDLE),
 optionsValid(false),
 nonblocking(false),
 setenvState(SETENV_STATE_IDLE),
 stdStrings(false),
 pguser(nullptr),
 dbName(nullptr),
 verbosity(PQERRORS_DEFAULT),
 showContext(PQSHOW_CONTEXT_ERRORS),
 pversion(PG_PROTOCOL(3, 0)),
 inStart(0),
 inCursor(0)
{

}

PGconn::~PGconn()
{

}

static const PQEnvironmentOption EnvironmentOptions[] =
{
	/* common user-interface settings */
	{
		"PGDATESTYLE", "datestyle"
	},
	{
		"PGTZ", "timezone"
	},
	/* internal performance-related settings */
	{
		"PGGEQO", "geqo"
	},
	{
		nullptr, nullptr
	}
};

int buildStartupPacket(const std::shared_ptr<PGconn> &conn, char *packet, const PQEnvironmentOption *options)
{
	int packetLen = 0;
	const PQEnvironmentOption *next;
	const char *val;

	/* Protocol version comes first. */
	if (packet)
	{
		uint32_t pv = Socket::hostToNetwork32(conn->pversion);
		memcpy(packet + packetLen, &pv, sizeof(uint32_t));
	}
	packetLen += sizeof(uint32_t);

	/* Add user name, database name, options */

#define ADD_STARTUP_OPTION(optname, optval) \
	do { \
		if (packet) \
			strcpy(packet + packetLen, optname); \
		packetLen += strlen(optname) + 1; \
		if (packet) \
			strcpy(packet + packetLen, optval); \
		packetLen += strlen(optval) + 1; \
	} while(0)

	if (conn->pguser)
		ADD_STARTUP_OPTION("user", conn->pguser);
	if (conn->dbName)
		ADD_STARTUP_OPTION("database", conn->dbName);

	/* Add trailing terminator */
	if (packet)
		packet[packetLen] = '\0';

	packetLen++;
	return packetLen;
}

char *pqBuildStartupPacket3(const std::shared_ptr<PGconn> &conn, int *packetlen,
		const PQEnvironmentOption *options)
{
	*packetlen = buildStartupPacket(conn, nullptr, options);
	char *buffer = (char*)zmalloc(*packetlen + 4);
	*packetlen = buildStartupPacket(conn, buffer + 4, options);
	int len = Socket::hostToNetwork32(*packetlen  + 4);
	memcpy(buffer, &len, 4);
	*packetlen = *packetlen  + 4;
	return buffer;
}

int pqGetc(char *result, const std::shared_ptr<PGconn> &conn)
{
	if (conn->inCursor >= conn->inBuffer.readableBytes())
		return -1;

	*result = conn->inBuffer.peek()[conn->inCursor++];
	return 0;
}

int pqGetInt(int *result, size_t bytes, const std::shared_ptr<PGconn> &conn)
{
	uint16_t tmp2;
	uint32_t tmp4;

	switch (bytes)
	{
		case 2:
			if (conn->inCursor + 2 > conn->inBuffer.readableBytes())
				return -1;
			memcpy(&tmp2, conn->inBuffer.peek() + conn->inCursor, 2);
			conn->inCursor += 2;
			*result = (int) Socket::networkToHost16(tmp2);
			break;
		case 4:
			if (conn->inCursor + 4 > conn->inBuffer.readableBytes())
				return -1;
			memcpy(&tmp4, conn->inBuffer.peek() + conn->inCursor, 4);
			conn->inCursor += 4;
			*result = (int) Socket::networkToHost32(tmp4);
			break;
		default:
			return -1;
	}
	return 0;
}

std::shared_ptr<PGresult> PQmakeEmptyPGresult(const std::shared_ptr<PGconn> &conn, ExecStatusType status)
{
	std::shared_ptr<PGresult> result(new PGresult());
	result->status = status;
	switch (status)
	{
		case PGRES_EMPTY_QUERY:
		case PGRES_COMMAND_OK:
		case PGRES_TUPLES_OK:
		case PGRES_COPY_OUT:
		case PGRES_COPY_IN:
		case PGRES_COPY_BOTH:
		case PGRES_SINGLE_TUPLE:
			/* non-error cases */
			break;
		default:
			result->errMsg = conn->errorMessage;
			break;
	}
	return result;
}

int pqGets(std::string &buff, const std::shared_ptr<PGconn> &conn)
{
	/* Copy conn data to locals for faster search loop */
	const char *inBuffer = conn->inBuffer.peek();
	int inCursor = conn->inCursor;
	int inEnd = conn->inBuffer.readableBytes();
	int slen;

	while (inCursor < inEnd && inBuffer[inCursor])
		inCursor++;

	if (inCursor >= inEnd)
		return -1;

	buff.clear();

	slen = inCursor - conn->inCursor;
	buff.append(inBuffer  + conn->inCursor, slen);
	conn->inCursor = ++inCursor;
	return 0;
}

int pqGetErrorNotice3(const std::shared_ptr<PGconn> &conn, bool isError)
{
	std::shared_ptr<PGresult> res;
	bool havePosition = false;
	std::string workBuf;
	char id;
	if (isError)
	{
		if (conn->result)
		{
			conn->result.reset();
		}
	}
}

int getNotify(const std::shared_ptr<PGconn> &conn)
{
	int bePid;
	std::shared_ptr<PGnotify> newNotify(new PGnotify());
	if (pqGetInt(&bePid, 4, conn))
		return -1;
	if (pqGets(conn->workBuffer, conn))
		return -1;
	newNotify->relname =  conn->workBuffer;
	if (pqGets(conn->workBuffer, conn))
		return -1;

	newNotify->bePid = bePid;
	conn->notify.push_back(newNotify);
}

int pqSkipnchar(size_t len, const std::shared_ptr<PGconn> &conn)
{
	if (len > (size_t) (conn->inBuffer.readableBytes() - conn->inCursor))
		return -1;

	conn->inCursor += len;
	return 0;
}

int getReadyForQuery(const std::shared_ptr<PGconn> &conn)
{
	char xactStatus;

	if (pqGetc(&xactStatus, conn))
		return -1;
	switch (xactStatus)
	{
		case 'I':
			conn->xactStatus = PQTRANS_IDLE;
			break;
		case 'T':
			conn->xactStatus = PQTRANS_INTRANS;
			break;
		case 'E':
			conn->xactStatus = PQTRANS_INERROR;
			break;
		default:
			conn->xactStatus = PQTRANS_UNKNOWN;
			break;
	}
	return 0;
}


void pqSaveParameterStatus(const std::shared_ptr<PGconn> &conn, const char *name, const char *value)
{
	for (auto it = conn->pstatus.begin(); it != conn->pstatus.end(); ++it)
	{
		if (strcmp((*it)->name.c_str(), name) == 0)
		{
			conn->pstatus.erase(it);
			break;
		}
	}

	std::shared_ptr<PGParameterStatus> status(new PGParameterStatus());
	status->name = name;
	status->value = value;
	conn->pstatus.push_back(status);
}

int getRowDescriptions(const std::shared_ptr<PGconn> &conn, int msgLength)
{
	std::shared_ptr<PGresult> result;
	int nfields;
	std::string errMsg;
	int i;

	/*
	 * When doing Describe for a prepared statement, there'll already be a
	 * PGresult created by getParamDescriptions, and we should fill data into
	 * that.  Otherwise, create a new, empty PGresult.
	 */
	if (conn->queryclass == PGQUERY_DESCRIBE)
	{
		if (conn->result)
			result = conn->result;
		else
			result = PQmakeEmptyPGresult(conn, PGRES_COMMAND_OK);
	}
	else
	{
		result = PQmakeEmptyPGresult(conn, PGRES_TUPLES_OK);
	}

	/* parseInput already read the 'T' label and message length. */
	/* the next two bytes are the number of fields */
	if (pqGetInt(&(result->numAttributes), 2, conn))
	{
		/* We should not run out of data here, so complain */
		errMsg = "insufficient data in \"T\" message";
		goto error;
	}

	nfields = result->numAttributes;
	/* allocate space for the attribute descriptors */
	if (nfields > 0)
	{
		std::shared_ptr<PGresAttDesc> desc(new PGresAttDesc());
		result->attDescs.push_back(desc);
	}

	/* result->binary is true only if ALL columns are binary */
	result->binary = (nfields > 0) ? 1 : 0;
	/* get type info */
	for (i = 0; i < nfields; i++)
	{
		int tableid;
		int columnid;
		int typid;
		int typlen;
		int atttypmod;
		int format;

		if (pqGets(conn->workBuffer, conn) ||
			pqGetInt(&tableid, 4, conn) ||
			pqGetInt(&columnid, 2, conn) ||
			pqGetInt(&typid, 4, conn) ||
			pqGetInt(&typlen, 2, conn) ||
			pqGetInt(&atttypmod, 4, conn) ||
			pqGetInt(&format, 2, conn))
		{
			/* We should not run out of data here, so complain */
			errMsg = "insufficient data in \"T\" message";
			goto error;
		}

		/*
		 * Since pqGetInt treats 2-byte integers as unsigned, we need to
		 * coerce these results to signed form.
		 */
		columnid = (int) ((int16_t) columnid);
		typlen = (int) ((int16_t) typlen);
		format = (int) ((int16_t) format);

		result->attDescs[i]->name =  conn->workBuffer;
		result->attDescs[i]->tableid = tableid;
		result->attDescs[i]->columnid = columnid;
		result->attDescs[i]->format = format;
		result->attDescs[i]->typid = typid;
		result->attDescs[i]->typlen = typlen;
		result->attDescs[i]->atttypmod = atttypmod;

		if (format != 1)
			result->binary = 0;
	}

	/* Sanity check that we absorbed all the data */
	if (conn->inCursor != conn->inStart + 5 + msgLength)
	{
		errMsg = "extraneous data in \"T\" message";
		goto error;
	}

	conn->inStart = conn->inCursor;
	/* Success! */
	conn->result = result;

	/* Advance inStart to show that the "T" message has been processed. */

	/*
	 * If we're doing a Describe, we're done, and ready to pass the result
	 * back to the client.
	 */
	if (conn->queryclass == PGQUERY_DESCRIBE)
	{
		conn->asyncStatus = PGASYNC_READY;
		return 0;
	}

	/*
	 * We could perform additional setup for the new result set here, but for
	 * now there's nothing else to do.
	 */

	/* And we're done. */
	return 0;

error:
	assert(false);
	return 0;
}

int getParameterStatus(const std::shared_ptr<PGconn> &conn)
{
	std::string valueBuf;
	/* Get the parameter name */
	if (pqGets(conn->workBuffer, conn))
		return -1;

	if (pqGets(valueBuf, conn))
	{
		return -1;
	}

	pqSaveParameterStatus(conn, conn->workBuffer.c_str(), valueBuf.c_str());
	return 0;
}

bool pqAddTuple(std::shared_ptr<PGresult> res,std::vector<std::string> tup, std::string &errmsgp)
{
	res->tuples.push_back(tup);
	return true;
}

int pqRowProcessor(const std::shared_ptr<PGconn> &conn, std::string &errmsgp)
{
	std:shared_ptr<PGresult> res = conn->result;
	int nfields = res->numAttributes;
	std::vector<std::string> columns = conn->rowBuffer;
	std::vector<std::string> tup;
	int i;

	for (i = 0; i < nfields; i++)
	{
		tup.push_back(columns[i]);
	}

	/* And add the tuple to the PGresult's tuple array */
	if (!pqAddTuple(res, tup, errmsgp))
		return 0;

	return 1;
}

int getCopyStart(const std::shared_ptr<PGconn> &conn, ExecStatusType copytype)
{
	std::shared_ptr<PGresult>  result;
	int nfields;
	int i;

	result = PQmakeEmptyPGresult(conn, copytype);
	/* the next two bytes are the number of fields	*/
	if (pqGetInt(&(result->numAttributes), 2, conn))
		return -1;
	nfields = result->numAttributes;

	/* allocate space for the attribute descriptors */
	if (nfields > 0)
	{
		std::shared_ptr<PGresAttDesc> desc(new PGresAttDesc());
	}

	for (i = 0; i < nfields; i++)
	{
		int format;

		if (pqGetInt(&format, 2, conn))
			return -1;

		/*
		 * Since pqGetInt treats 2-byte integers as unsigned, we need to
		 * coerce these results to signed form.
		 */
		format = (int) ((int16_t) format);
		result->attDescs[i]->format = format;
	}

	/* Success! */
	conn->result = result;
	return 0;
}

int getAnotherTuple(const std::shared_ptr<PGconn> &conn, int msgLength)
{
	std::shared_ptr<PGresult> result = conn->result;
	int nfields = result->numAttributes;
	std::string errMsg;
	int tupnfields;		/* # fields from tuple */
	int vlen;			/* length of the current field value */
	int i;

	/* Get the field count and make sure it's what we expect */
	if (pqGetInt(&tupnfields, 2, conn))
	{
		/* We should not run out of data here, so complain */
		errMsg = "insufficient data in \"D\" message";
		goto error;
	}

	if (tupnfields != nfields)
	{
		errMsg = "unexpected field count in \"D\" message";
		goto error;
	}

	/* Scan the fields */
	for (i = 0; i < nfields; i++)
	{
		/* get the value length */
		if (pqGetInt(&vlen, 4, conn))
		{
			/* We should not run out of data here, so complain */
			errMsg = "insufficient data in \"D\" message";
			goto error;
		}

		/*
		 * rowbuf[i].value always points to the next address in the data
		 * buffer even if the value is NULL.  This allows row processors to
		 * estimate data sizes more easily.
		 */
		conn->rowBuffer.push_back(std::string(conn->inBuffer.peek() + conn->inCursor, vlen));

		/* Skip over the data value */
		if (vlen > 0)
		{
			if (pqSkipnchar(vlen, conn))
			{
				/* We should not run out of data here, so complain */
				errMsg = "insufficient data in \"D\" message";
				goto error;
			}
		}
	}

	/* Sanity check that we absorbed all the data */
	if (conn->inCursor != conn->inStart + 5 + msgLength)
	{
		errMsg = "extraneous data in \"D\" message";
		goto error;
	}

	/* Advance inStart to show that the "D" message has been processed. */
	conn->inStart = conn->inCursor;

	/* Advance inStart to show that the "D" message has been processed. */

	if (pqRowProcessor(conn, errMsg))
		return 0;				/* normal, successful exit */
error:
	assert(false);
	return 0;
}

void pqParseInput3(const std::shared_ptr<PGconn> &conn)
{
	char id;
	int msgLength;
	int avail;

	/*
		 * Loop to parse successive complete messages available in the buffer.
		 */
	for (;;)
	{
		/*
		 * Try to read a message.  First get the type code and length. Return
		 * if not enough data.
		 */

		conn->inCursor = conn->inStart;
		if (pqGetc(&id, conn))
			return;
		if (pqGetInt(&msgLength, 4, conn))
			return;

		/*
		 * Try to validate message type/length here.  A length less than 4 is
		 * definitely broken.  Large lengths should only be believed for a few
		 * message types.
		 */
		if (msgLength < 4)
		{
			assert(false);
		}

		if (msgLength > 30000 && !VALID_LONG_MESSAGE_TYPE(id))
		{
			assert(false);
		}

		/*
		 * Can't process if message body isn't all here yet.
		 */
		msgLength -= 4;
		avail = conn->inBuffer.readableBytes() - conn->inCursor;
		if (avail < msgLength)
		{
			return;
		}

		if (id == 'A')
		{
			if (getNotify(conn))
				return;
		}
		else if (id == 'N')
		{
			//if (pqGetErrorNotice3(conn, false))
			//	return;
		}
		else if (conn->asyncStatus != PGASYNC_BUSY)
		{
			/* If not IDLE state, just wait ... */
			if (conn->asyncStatus != PGASYNC_IDLE)
				return;

			if (id == 'E')
			{
				//if (pqGetErrorNotice3(conn, false /* treat as notice */ ))
				//	return;
			}
			else if (id == 'S')
			{
				if (getParameterStatus(conn))
					return;
			}
			else
			{
				printf( "message type 0x%02x arrived from server while idle", id);
				conn->inCursor += msgLength;
			}
		}
		else
		{
			switch (id)
			{
				case 'C':		/* command complete */
					if (pqGets(conn->workBuffer, conn))
						return;
					if (conn->result == nullptr)
					{
						conn->result = PQmakeEmptyPGresult(conn,
														   PGRES_COMMAND_OK);
					}

					if (conn->result)
					{
						conn->result->cmdStatus = conn->workBuffer;
					}

					conn->asyncStatus = PGASYNC_READY;
					break;
				case 'E':		/* error return */
					//if (pqGetErrorNotice3(conn, true))
					//	return;
					conn->asyncStatus = PGASYNC_READY;
					break;
				case 'Z':		/* backend is ready for new query */
					if (getReadyForQuery(conn))
						return;
					conn->asyncStatus = PGASYNC_IDLE;
					break;
				case 'I':		/* empty query */
					if (conn->result == nullptr)
					{
						conn->result = PQmakeEmptyPGresult(conn,
														   PGRES_EMPTY_QUERY);
					}

					conn->asyncStatus = PGASYNC_READY;
					break;
				case '1':		/* Parse Complete */
					/* If we're doing PQprepare, we're done; else ignore */
					if (conn->queryclass == PGQUERY_PREPARE)
					{
						if (conn->result == nullptr)
						{
							conn->result = PQmakeEmptyPGresult(conn,
															   PGRES_COMMAND_OK);
						}
						conn->asyncStatus = PGASYNC_READY;
					}
					break;
				case '2':		/* Bind Complete */
				case '3':		/* Close Complete */
					/* Nothing to do for these message types */
					break;
				case 'S':		/* parameter status */
					if (getParameterStatus(conn))
						return;
					break;
				case 'K':		/* secret key data from the backend */

					/*
					 * This is expected only during backend startup, but it's
					 * just as easy to handle it as part of the main loop.
					 * Save the data and continue processing.
					 */
					if (pqGetInt(&(conn->bePid), 4, conn))
						return;
					if (pqGetInt(&(conn->beKey), 4, conn))
						return;
					break;
				case 'T':		/* Row Description */
					if (conn->result != nullptr &&
						conn->result->status == PGRES_FATAL_ERROR)
					{
						/*
						 * We've already choked for some reason.  Just discard
						 * the data till we get to the end of the query.
						 */
						conn->inCursor += msgLength;
					}
					else if (conn->result == nullptr ||
							 conn->queryclass == PGQUERY_DESCRIBE)
					{
						/* First 'T' in a query sequence */
						if (getRowDescriptions(conn, msgLength))
							return;
						/* getRowDescriptions() moves inStart itself */
						continue;
					}
					else
					{
						/*
						 * A new 'T' message is treated as the start of
						 * another PGresult.  (It is not clear that this is
						 * really possible with the current backend.) We stop
						 * parsing until the application accepts the current
						 * result.
						 */
						conn->asyncStatus = PGASYNC_READY;
						return;
					}
					break;
				case 'n':		/* No Data */

					/*
					 * NoData indicates that we will not be seeing a
					 * RowDescription message because the statement or portal
					 * inquired about doesn't return rows.
					 *
					 * If we're doing a Describe, we have to pass something
					 * back to the client, so set up a COMMAND_OK result,
					 * instead of TUPLES_OK.  Otherwise we can just ignore
					 * this message.
					 */
					if (conn->queryclass == PGQUERY_DESCRIBE)
					{
						if (conn->result == nullptr)
						{
							conn->result = PQmakeEmptyPGresult(conn,
															   PGRES_COMMAND_OK);
						}
						conn->asyncStatus = PGASYNC_READY;
					}
					break;
				case 't':		/* Parameter Description */
					//if (getParamDescriptions(conn, msgLength))
					//	return;
					/* getParamDescriptions() moves inStart itself */
					continue;
				case 'D':		/* Data Row */
					if (conn->result != nullptr &&
						conn->result->status == PGRES_TUPLES_OK)
					{
						/* Read another tuple of a normal query response */
						if (getAnotherTuple(conn, msgLength))
							return;
						/* getAnotherTuple() moves inStart itself */
						continue;
					}
					else if (conn->result != nullptr &&
							 conn->result->status == PGRES_FATAL_ERROR)
					{
						/*
						 * We've already choked for some reason.  Just discard
						 * tuples till we get to the end of the query.
						 */
						conn->inCursor += msgLength;
					}
					else
					{
						/* Set up to report error at end of query */
						conn->errorMessage = "server sent data (\"D\" message) without prior row description (\"T\" message)\n";
						/* Discard the unexpected message */
						conn->inCursor += msgLength;
					}
					break;
				case 'G':		/* Start Copy In */
					if (getCopyStart(conn, PGRES_COPY_IN))
						return;
					conn->asyncStatus = PGASYNC_COPY_IN;
					break;
				case 'H':		/* Start Copy Out */
					if (getCopyStart(conn, PGRES_COPY_OUT))
						return;
					conn->asyncStatus = PGASYNC_COPY_OUT;
					break;
				case 'W':		/* Start Copy Both */
					if (getCopyStart(conn, PGRES_COPY_BOTH))
						return;
					conn->asyncStatus = PGASYNC_COPY_BOTH;
					break;
				case 'd':		/* Copy Data */

					/*
					 * If we see Copy Data, just silently drop it.  This would
					 * only occur if application exits COPY OUT mode too
					 * early.
					 */
					conn->inCursor += msgLength;
					break;
				case 'c':		/* Copy Done */

					/*
					 * If we see Copy Done, just silently drop it.  This is
					 * the normal case during PQendcopy.  We will keep
					 * swallowing data, expecting to see command-complete for
					 * the COPY command.
					 */
					break;
				default:
					conn->errorMessage = "unexpected response from server; first received character was ";
					conn->errorMessage += std::to_string(id);
					conn->errorMessage += "\n";

					/* build an error result holding the error message */
					//pqSaveErrorResult(conn);
					/* not sure if we will see more, so go to ready state */
					conn->asyncStatus = PGASYNC_READY;
					/* Discard the unexpected message */
					conn->inCursor += msgLength;
					break;
			}					/* switch on protocol character */
		}

		/* Successfully consumed this message */
		if (conn->inCursor ==  conn->inStart + 5 + msgLength)
		{
			/* Normal case: parsing agrees with specified length */

			/* Normal case: parsing agrees with specified length */
			conn->inBuffer.retrieve(conn->inCursor);
			conn->inCursor = 0;
			conn->inStart = 0;
		}
		else
		{
			/* Trouble --- report it */
			conn->errorMessage = "message contents do not agree with length in message type ";
			conn->errorMessage += std::to_string(id);
			conn->errorMessage += "\n";

			/* build an error result holding the error message */
			conn->asyncStatus = PGASYNC_READY;
			conn->result = PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
			/* trust the specified message length as what to skip */
			conn->inBuffer.retrieve(5 + msgLength);
			conn->inStart += 5 + msgLength;
		}
	}
}

int pqPuts(const char *s, const std::shared_ptr<PGconn> &conn)
{
	conn->outBuffer.appendInt32(strlen(s) + 1 + 4);
	conn->outBuffer.append(s);
	return 0;
}

int pqPutMsgEnd(const std::shared_ptr<PGconn> &conn)
{
	return 0;
}

int pqPutMsgStart(char msgType, bool force, const std::shared_ptr<PGconn> &conn)
{
	/* okay, save the message type byte if any */
	if (msgType)
		conn->outBuffer.append(&msgType, 1);
	return 0;
}

int PQputCopyEnd(const std::shared_ptr<PGconn> &conn, std::string &errormsg)
{
	if (conn->asyncStatus != PGASYNC_COPY_IN &&
		conn->asyncStatus != PGASYNC_COPY_BOTH)
	{
		conn->errorMessage = "no COPY in progress\n";
		return -1;
	}

	/*
	 * Send the COPY END indicator.  This is simple enough that we don't
	 * bother delegating it to the fe-protocol files.
	 */
	if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3)
	{
		if (errormsg.size() > 0)
		{
			/* Send COPY FAIL */
			if (pqPutMsgStart('f', false, conn) < 0 ||
				pqPuts(errormsg.c_str(), conn) < 0 ||
				pqPutMsgEnd(conn) < 0)
				return -1;
		}
		else
		{
			/* Send COPY DONE */
			if (pqPutMsgStart('c', false, conn) < 0 ||
				pqPutMsgEnd(conn) < 0)
				return -1;
		}

		/*
		 * If we sent the COPY command in extended-query mode, we must issue a
		 * Sync as well.
		 */
		if (conn->queryclass != PGQUERY_SIMPLE)
		{
			if (pqPutMsgStart('S', false, conn) < 0 ||
				pqPutMsgEnd(conn) < 0)
				return -1;
		}
	}
	return 1;
}

int pqReadData(const std::shared_ptr<PGconn> &conn)
{
	int savedErrno = 0;
	ssize_t n = conn->inBuffer.readFd(conn->sockfd, &savedErrno);
	if (n > 0)
	{

	}
	else if (n == 0)
	{
		return -1;
	}
	else
	{
		if ((savedErrno == EAGAIN) || (savedErrno == EINTR))
		{
			/* Try again later */
		}
		else
		{
			return -1;
		}
	}
	return 1;
}

std::shared_ptr<PGresult> pqPrepareAsyncResult(const std::shared_ptr<PGconn> &conn)
{
	std::shared_ptr<PGresult> res;

	/*
	 * conn->result is the PGresult to return.  If it is NULL (which probably
	 * shouldn't happen) we assume there is an appropriate error message in
	 * conn->errorMessage.
	 */
	res = conn->result;
	if (!res)
		res = PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
	else
	{
		/*
		 * Make sure PQerrorMessage agrees with result; it could be different
		 * if we have concatenated messages.
		 */
		conn->errorMessage  = res->errMsg;
	}
	return res;
}

 std::shared_ptr<PGresult> getCopyResult(const std::shared_ptr<PGconn> &conn, ExecStatusType copytype)
{
	/*
	 * If the server connection has been lost, don't pretend everything is
	 * hunky-dory; instead return a PGRES_FATAL_ERROR result, and reset the
	 * asyncStatus to idle (corresponding to what we'd do if we'd detected I/O
	 * error in the earlier steps in PQgetResult).  The text returned in the
	 * result is whatever is in conn->errorMessage; we hope that was filled
	 * with something relevant when the lost connection was detected.
	 */
	if (conn->status != CONNECTION_OK)
	{
		conn->asyncStatus = PGASYNC_IDLE;
		return pqPrepareAsyncResult(conn);
	}

	/* If we have an async result for the COPY, return that */
	if (conn->result && conn->result->status == copytype)
		return pqPrepareAsyncResult(conn);

	/* Otherwise, invent a suitable PGresult */
	return PQmakeEmptyPGresult(conn, copytype);
}

std::shared_ptr<PGresult> PQgetResult(const std::shared_ptr<PGconn> &conn)
{
	std::shared_ptr<PGresult> res;
	if (!conn)
	{
		return nullptr;
	}

	/* Parse any available data, if our state permits. */
	pqParseInput3(conn);

	/* If not ready to return something, block until we are. */
	while (conn->asyncStatus == PGASYNC_BUSY)
	{
		int flushResult;

		/*
		 * If data remains unsent, send it.  Else we might be waiting for the
		 * result of a command the backend hasn't even got yet.
		 */

		/* Wait for some more data, and load it. */
		if (flushResult ||
			pqReadData(conn) < 0)
		{
			conn->asyncStatus = PGASYNC_IDLE;
			return nullptr;
		}

		/* Parse it. */
		pqParseInput3(conn);
	}

	/* Return the appropriate thing. */
	switch (conn->asyncStatus)
	{
		case PGASYNC_IDLE:
			res = nullptr;			/* query is complete */
			break;
		case PGASYNC_READY:
			res = pqPrepareAsyncResult(conn);
			/* Set the state back to BUSY, allowing parsing to proceed. */
			conn->asyncStatus = PGASYNC_BUSY;
			break;
		case PGASYNC_COPY_IN:
			res = getCopyResult(conn, PGRES_COPY_IN);
			break;
		case PGASYNC_COPY_OUT:
			res = getCopyResult(conn, PGRES_COPY_OUT);
			break;
		case PGASYNC_COPY_BOTH:
			res = getCopyResult(conn, PGRES_COPY_BOTH);
			break;
		default:
			res = PQmakeEmptyPGresult(conn, PGRES_FATAL_ERROR);
			break;
	}
	return res;
}

int pqSendSome(const std::shared_ptr<PGconn> &conn)
{
	int32_t nwritten = 0;
	while (conn->outBuffer.readableBytes() > 0)
	{
		nwritten = Socket::write(conn->sockfd, conn->outBuffer.peek(), conn->outBuffer.readableBytes());
		if (nwritten < 0)
		{
			if ((errno == EAGAIN) || (errno == EINTR))
			{
				/* Try again later */
			}
			else
			{
				return -1;
			}
		}
		else if (nwritten > 0)
		{
			conn->outBuffer.retrieve(nwritten);
		}
	}
	return 1;
}

int pqFlush(const std::shared_ptr<PGconn> &conn)
{
	return pqSendSome(conn);
}

int PQsendQuery(const std::shared_ptr<PGconn> &conn, const char *query)
{
	/* construct the outgoing Query message */
	if (pqPutMsgStart('Q', false, conn) < 0 ||
		pqPuts(query, conn) < 0)
	{
		return 0;
	}

	/* remember we are using simple query protocol */
	conn->queryclass = PGQUERY_SIMPLE;
	/*
	 * Give the data a push.  In nonblock mode, don't complain if we're unable
	 * to send it all; PQgetResult() will do any additional flushing needed.
	 */
	if (pqFlush(conn) < 0)
	{
		return 0;
	}

	/* OK, it's launched! */
	conn->asyncStatus = PGASYNC_BUSY;
	return 1;
}

bool PQexecStart(const std::shared_ptr<PGconn> &conn)
{
	std::shared_ptr<PGresult> result;
	if (!conn)
	{
		return false;
	}

	/*
	 * Silently discard any prior query result that application didn't eat.
	 * This is probably poor design, but it's here for backward compatibility.
	 */
	while ((result = PQgetResult(conn)) != nullptr)
	{
		ExecStatusType resultStatus = result->status;
		result.reset();
		if (resultStatus == PGRES_COPY_IN)
		{
			if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3)
			{
				std::string msg = "COPY terminated by new PQexec";
				/* In protocol 3, we can get out of a COPY IN state */
				if (PQputCopyEnd(conn, msg) < 0)
					return false;
				/* keep waiting to swallow the copy's failure message */
			}
			else
			{
				/* In older protocols we have to punt */
				conn->errorMessage = "COPY IN state must be terminated first\n";
				return false;
			}
		}
		else if (resultStatus == PGRES_COPY_OUT)
		{
			if (PG_PROTOCOL_MAJOR(conn->pversion) >= 3)
			{
				/*
				 * In protocol 3, we can get out of a COPY OUT state: we just
				 * switch back to BUSY and allow the remaining COPY data to be
				 * dropped on the floor.
				 */
				conn->asyncStatus = PGASYNC_BUSY;
				/* keep waiting to swallow the copy's completion message */
			}
			else
			{
				/* In older protocols we have to punt */
				conn->errorMessage = "COPY OUT state must be terminated first\n";
				return false;
			}
		}
		else if (resultStatus == PGRES_COPY_BOTH)
		{
			/* We don't allow PQexec during COPY BOTH */
			conn->errorMessage = "PQexec not allowed during COPY BOTH\n";
			return false;
		}
		/* check for loss of connection, too */
		if (conn->status == CONNECTION_BAD)
			return false;
	}
	return true;
}

std::shared_ptr<PGresult> PQexecFinish(const std::shared_ptr<PGconn> &conn)
{
	std::shared_ptr<PGresult> result;
	std::shared_ptr<PGresult> lastResult;

	/*
	 * For backwards compatibility, return the last result if there are more
	 * than one --- but merge error messages if we get more than one error
	 * result.
	 *
	 * We have to stop if we see copy in/out/both, however. We will resume
	 * parsing after application performs the data transfer.
	 *
	 * Also stop if the connection is lost (else we'll loop infinitely).
	 */
	lastResult = nullptr;
	while ((result = PQgetResult(conn)) != nullptr)
	{
		if (lastResult)
		{
			if (lastResult->status == PGRES_FATAL_ERROR &&
				result->status == PGRES_FATAL_ERROR)
			{
				result = lastResult;

				/*
				 * Make sure PQerrorMessage agrees with concatenated result
				 */

			}
		}

		lastResult = result;
		if (result->status == PGRES_COPY_IN ||
			result->status == PGRES_COPY_OUT ||
			result->status == PGRES_COPY_BOTH ||
			conn->status == CONNECTION_BAD)
			break;
	}
	return lastResult;
}

std::shared_ptr<PGresult> PQexec(const std::shared_ptr<PGconn> &conn, const char *query)
{
	if (!PQexecStart(conn))
		return nullptr;
	if (!PQsendQuery(conn, query))
		return nullptr;
	return PQexecFinish(conn);
}

std::shared_ptr<PGconn> PQconnectStart(const char *conninfo)
{
	std::shared_ptr<PGconn> conn(new PGconn);
	int sockfd = Socket::createSocket();
	if (sockfd == -1)
	{
		return nullptr;
	}

	int ret = Socket::connect(sockfd, "127.0.0.1", 5432);
	if (ret == -1)
	{
		return nullptr;
	}

	conn->sockfd = sockfd;
	conn->pghost = "127.0.0.1";
	conn->pgport = 5432;
	conn->status = CONNECTION_OK;
	conn->pguser = "postgres";
	conn->dbName = "postgres";

	int packetlen = 0;
	char *buffer = pqBuildStartupPacket3(conn, &packetlen, EnvironmentOptions);
	while(1)
	{
		ssize_t n = Socket::write(conn->sockfd, buffer, packetlen);
		if (n < 0)
		{
			if (errno == EAGAIN || (errno == EINTR))
			{
				/* Try again later */
			}
			else
			{
				return nullptr;
			}
		}
		else
		{
			break;
		}
	}

	zfree(buffer);
	conn->status = CONNECTION_AWAITING_RESPONSE;

	while(1)
	{
		char buffer[65536];
		ssize_t n = Socket::read(conn->sockfd, buffer, sizeof(buffer));
		if (n == 0)
		{
			return nullptr;
		}

		if (n < 0)
		{
			return nullptr;
		}
		else
		{
			break;
		}
	}

	return conn;
}

