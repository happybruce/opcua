#ifndef BACKEND_H
#define BACKEND_H

#include <time.h>
#include <stdlib.h>
#include "open62541.h"
#include "sqlite3.h"


static const size_t END_OF_DATA = SIZE_MAX;
static const size_t QUERY_BUFFER_SIZE = 500;


UA_Int64 convertTimestampStringToUnixSeconds(const char* timestampString)
{
    UA_DateTimeStruct dts;
    memset(&dts, 0, sizeof(dts));

    sscanf(timestampString, "%hu-%hu-%hu %hu:%hu:%hu", 
        &dts.year, &dts.month, &dts.day, &dts.hour, &dts.min, &dts.sec);

    UA_DateTime dt = UA_DateTime_fromStruct(dts);

    UA_Int64 t = UA_DateTime_toUnixTime(dt);

    return t;
}


const char* convertUnixSecondsToTimestampString(UA_Int64 unixSeconds)
{
    static char buffer[20];


    UA_DateTime dt = UA_DateTime_fromUnixTime(unixSeconds);
    UA_DateTimeStruct dts = UA_DateTime_toStruct(dt);

    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    tm.tm_year = dts.year - 1900;
    tm.tm_mon  = dts.month - 1;
    tm.tm_mday = dts.day;
    tm.tm_hour = dts.hour;
    tm.tm_min  = dts.min;
    tm.tm_sec  = dts.sec;

    memset(buffer, 0, 20);

    strftime(buffer, 20, "%Y-%m-%d %H:%M:%S", &tm);

    return buffer;
}



//Context that is needed for the SQLite callback for copying data.
struct context_copyDataValues {
    size_t maxValues;
    size_t counter;
    UA_DataValue *values;
};

typedef  struct context_copyDataValues  context_copyDataValues;


struct context_sqlite {

    sqlite3* sqlite;

    const char* measuringPointID;
};


static struct context_sqlite*
generateContext_sqlite(const char* filename) 
{

    sqlite3* handle;
    char *errorMessage;

    int res = sqlite3_open(filename, &handle);

    if (res != SQLITE_OK)
        return NULL;

    struct context_sqlite* ret = (struct context_sqlite*)UA_calloc(1, sizeof(struct context_sqlite));
    if (ret == NULL)
    {
        return NULL;
    }

    const char *sql = "DROP TABLE IF EXISTS PeriodicValues;" 
                      "CREATE TABLE PeriodicValues(MeasuringPointID INT, Value DOUBLE, Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);";

    
    res = sqlite3_exec(handle, sql, NULL, NULL, &errorMessage);
    if (res != SQLITE_OK)
    {
        printf("%s | Error | %s\n", __func__, errorMessage);
        sqlite3_free(errorMessage);
        sqlite3_close(handle);

        return NULL;
    }


    ret->sqlite = handle;

    //For this demo we have only one source measuring point which we hardcode in the context.
    //A more advanced demo should determine the available measuring points from the source
    //itself or maybe an external configuration file.
    ret->measuringPointID = "1";

    return ret;
}


static UA_StatusCode
serverSetHistoryData_sqliteHDB(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId,
	UA_Boolean historizing,
	const UA_DataValue *value)
{
    struct context_sqlite* context = (struct context_sqlite*)hdbContext;

    size_t result;
    char* errorMessage;

    char query[QUERY_BUFFER_SIZE];
    strncpy(query, "INSERT INTO PeriodicValues VALUES(1, ", QUERY_BUFFER_SIZE);
    if (value->hasValue && 
        value->status == UA_STATUSCODE_GOOD &&
        value->value.type == &UA_TYPES[UA_TYPES_DOUBLE])
    {
        char remaining[30];
        snprintf(remaining, 30, "%f, CURRENT_TIMESTAMP);", *(double*)(value->value.data));
        strncat(query, remaining, QUERY_BUFFER_SIZE);
    }
    else
    {
        printf("%s | Error | historical value is invalid\n", __func__);
        return UA_STATUSCODE_BADINTERNALERROR;
    }

    int res = sqlite3_exec(context->sqlite, query, NULL, NULL, &errorMessage);
    if (res != SQLITE_OK)
    {
        printf("%s | Error | %s\n", __func__, errorMessage);
        sqlite3_free(errorMessage);

        return UA_STATUSCODE_BADINTERNALERROR;
    }

    return UA_STATUSCODE_GOOD;
}


static size_t
getEnd_sqliteHDB(UA_Server *server,
	void *hdbContext,
	const UA_NodeId *sessionId,
	void *sessionContext,
	const UA_NodeId *nodeId)
{
    return END_OF_DATA;
}


//This is a callback for all queries that return a single timestamp as the number of Unix seconds
static int timestamp_callback(void* result, int count, char **data, char **columns)
{
    *(UA_Int64*)result = convertTimestampStringToUnixSeconds(data[0]);

    return 0;
}


static int resultSize_callback(void* result, int count, char **data, char **columns)
{
    *(size_t*)result = strtol(data[0], NULL, 10);

    return 0;
}


static size_t
lastIndex_sqliteHDB(UA_Server *server,
    void *hdbContext,
    const UA_NodeId *sessionId,
    void *sessionContext,
    const UA_NodeId *nodeId)
{
    struct context_sqlite* context = (struct context_sqlite*)hdbContext;

    size_t result;
    char* errorMessage;

    char query[QUERY_BUFFER_SIZE];
    strncpy(query, "SELECT Timestamp FROM PeriodicValues WHERE MeasuringPointID=", QUERY_BUFFER_SIZE);
    strncat(query, context->measuringPointID, QUERY_BUFFER_SIZE);
    strncat(query, " ORDER BY Timestamp DESC LIMIT 1", QUERY_BUFFER_SIZE);

    int res = sqlite3_exec(context->sqlite, query, timestamp_callback, &result, &errorMessage);

    if (res != SQLITE_OK)
    {
        printf("%s | Error | %s\n", __func__, errorMessage);
        sqlite3_free(errorMessage);
        return END_OF_DATA;
    }

    return result;
}

static size_t
firstIndex_sqliteHDB(UA_Server *server,
    void *hdbContext,
    const UA_NodeId *sessionId,
    void *sessionContext,
    const UA_NodeId *nodeId)
{
    struct context_sqlite* context = (struct context_sqlite*)hdbContext;

    size_t result;
    char* errorMessage;

    char query[QUERY_BUFFER_SIZE];
    strncpy(query, "SELECT Timestamp FROM PeriodicValues WHERE MeasuringPointID=", QUERY_BUFFER_SIZE);
    strncat(query, context->measuringPointID, QUERY_BUFFER_SIZE);
    strncat(query, " ORDER BY Timestamp LIMIT 1", QUERY_BUFFER_SIZE);

    int res = sqlite3_exec(context->sqlite, query, timestamp_callback, &result, &errorMessage);

    if (res != SQLITE_OK)
    {
        printf("%s | Error | %s\n", __func__, errorMessage);
        sqlite3_free(errorMessage);
        return END_OF_DATA;
    }

    return result;
}


static UA_Boolean
search_sqlite(struct context_sqlite* context,
	UA_Int64 unixSeconds, MatchStrategy strategy,
	size_t *index) 
{	
    *index = END_OF_DATA; // TODO
    char* errorMessage;

    char query[QUERY_BUFFER_SIZE];
    strncpy(query, "SELECT Timestamp FROM PeriodicValues WHERE MeasuringPointID=", QUERY_BUFFER_SIZE);
    strncat(query, context->measuringPointID, QUERY_BUFFER_SIZE);
    strncat(query, " AND ", QUERY_BUFFER_SIZE);

    switch (strategy)
    {
    case MATCH_EQUAL_OR_AFTER:
        strncat(query, "Timestamp>='", QUERY_BUFFER_SIZE);
        strncat(query, convertUnixSecondsToTimestampString(unixSeconds), QUERY_BUFFER_SIZE);
        strncat(query, "' ORDER BY Timestamp LIMIT 1", QUERY_BUFFER_SIZE);
        break;
    case MATCH_AFTER:
        strncat(query, "Timestamp>'", QUERY_BUFFER_SIZE);
        strncat(query, convertUnixSecondsToTimestampString(unixSeconds), QUERY_BUFFER_SIZE);
        strncat(query, "' ORDER BY Timestamp LIMIT 1", QUERY_BUFFER_SIZE);
        break;
    case MATCH_EQUAL_OR_BEFORE:
        strncat(query, "Timestamp<='", QUERY_BUFFER_SIZE);
        strncat(query, convertUnixSecondsToTimestampString(unixSeconds), QUERY_BUFFER_SIZE);
        strncat(query, "' ORDER BY Timestamp DESC LIMIT 1", QUERY_BUFFER_SIZE);
        break;
    case MATCH_BEFORE:
        strncat(query, "Timestamp<'", QUERY_BUFFER_SIZE);
        strncat(query, convertUnixSecondsToTimestampString(unixSeconds), QUERY_BUFFER_SIZE);
        strncat(query, "' ORDER BY Timestamp DESC LIMIT 1", QUERY_BUFFER_SIZE);
        break;
    default:
        return false;
    }


    int res = sqlite3_exec(context->sqlite, query, timestamp_callback, index, &errorMessage);

    if (res != SQLITE_OK)
    {
        printf("%s | Error | %s\n", __func__, errorMessage);
        sqlite3_free(errorMessage);
        return false;
    }
    else
    {
        return true;
    }

}

static size_t
getDateTimeMatch_sqliteHDB(UA_Server *server,
    void *hdbContext,
    const UA_NodeId *sessionId,
    void *sessionContext,
    const UA_NodeId *nodeId,
    const UA_DateTime timestamp,
    const MatchStrategy strategy)
{
    struct context_sqlite* context = (struct context_sqlite*)hdbContext;

    UA_Int64 ts = UA_DateTime_toUnixTime(timestamp);

    size_t result = END_OF_DATA;

    UA_Boolean res = search_sqlite(context, ts, strategy, &result);

    return result;
}


static size_t
resultSize_sqliteHDB(UA_Server *server,
    void *hdbContext,
    const UA_NodeId *sessionId,
    void *sessionContext,
    const UA_NodeId *nodeId,
    size_t startIndex,
    size_t endIndex)
{
    struct context_sqlite* context = (struct context_sqlite*)hdbContext;

    char* errorMessage;
    size_t result = 0;

    char query[QUERY_BUFFER_SIZE];
    strncpy(query, "SELECT COUNT(*) FROM PeriodicValues WHERE ", QUERY_BUFFER_SIZE);
    strncat(query, "(Timestamp>='", QUERY_BUFFER_SIZE);
    strncat(query, convertUnixSecondsToTimestampString(startIndex), QUERY_BUFFER_SIZE);
    strncat(query, "') AND (Timestamp<='", QUERY_BUFFER_SIZE);
    strncat(query, convertUnixSecondsToTimestampString(endIndex), QUERY_BUFFER_SIZE);
    strncat(query, "') AND (MeasuringPointID=", QUERY_BUFFER_SIZE);
    strncat(query, context->measuringPointID, QUERY_BUFFER_SIZE);
    strncat(query, ")", QUERY_BUFFER_SIZE);

    int res = sqlite3_exec(context->sqlite, query, resultSize_callback, &result, &errorMessage);

    if (res != SQLITE_OK)
    {
        printf("%s | Error | %s\n", __func__, errorMessage);
        sqlite3_free(errorMessage);
        return 0; // no data
    }

    return result;
}


static int copyDataValues_callback(void* result, int count, char **data, char **columns)
{
    UA_DataValue dv;
    UA_DataValue_init(&dv);

    dv.status = UA_STATUSCODE_GOOD;
    dv.hasStatus = true;

    dv.sourceTimestamp = UA_DateTime_fromUnixTime(convertTimestampStringToUnixSeconds(data[0]));
    dv.hasSourceTimestamp = true;

    dv.serverTimestamp = dv.sourceTimestamp;
    dv.hasServerTimestamp = true;

    double value = strtod(data[1], NULL);

    UA_Variant_setScalarCopy(&dv.value, &value, &UA_TYPES[UA_TYPES_DOUBLE]);
    dv.hasValue = true;

    context_copyDataValues* ctx = (context_copyDataValues*)result;

    UA_DataValue_copy(&dv, &ctx->values[ctx->counter]);

    ctx->counter++;

    if (ctx->counter == ctx->maxValues)
    {
        return 1;
    }
    else
    {
        return 0;
    }
}


static UA_StatusCode
copyDataValues_sqliteHDB(UA_Server *server,
    void *hdbContext,
    const UA_NodeId *sessionId,
    void *sessionContext,
    const UA_NodeId *nodeId,
    size_t startIndex,
    size_t endIndex,
    UA_Boolean reverse,
    size_t maxValues,
    UA_NumericRange range,
    UA_Boolean releaseContinuationPoints,
    const UA_ByteString *continuationPoint,
    UA_ByteString *outContinuationPoint,
    size_t *providedValues,
    UA_DataValue *values)
{
    //NOTE: this demo does not support continuation points!!!
    struct context_sqlite* context = (struct context_sqlite*)hdbContext;

    char* errorMessage;
    const char* measuringPointID = "1";

    char query[QUERY_BUFFER_SIZE];
    strncpy(query, "SELECT Timestamp, Value FROM PeriodicValues WHERE ", QUERY_BUFFER_SIZE);
    strncat(query, "(Timestamp>='", QUERY_BUFFER_SIZE);
    strncat(query, convertUnixSecondsToTimestampString(startIndex), QUERY_BUFFER_SIZE);
    strncat(query, "') AND (Timestamp<='", QUERY_BUFFER_SIZE);
    strncat(query, convertUnixSecondsToTimestampString(endIndex), QUERY_BUFFER_SIZE);
    strncat(query, "') AND (MeasuringPointID=", QUERY_BUFFER_SIZE);
    strncat(query, measuringPointID, QUERY_BUFFER_SIZE);
    strncat(query, ")", QUERY_BUFFER_SIZE);

    context_copyDataValues ctx;
    ctx.maxValues = maxValues;
    ctx.counter = 0;
    ctx.values = values;

    int res = sqlite3_exec(context->sqlite, query, copyDataValues_callback, &ctx, &errorMessage);

    if (res != SQLITE_OK)
    {
        if (res == SQLITE_ABORT) // if reach maxValues, then request abort, so this is not error
        {
            sqlite3_free(errorMessage);
            return UA_STATUSCODE_GOOD;
        }
        else
        {
            printf("%s | Error | %s\n", __func__, errorMessage);
            sqlite3_free(errorMessage);
            return UA_STATUSCODE_BADINTERNALERROR;
        }
            
    }
    else
    {
        return UA_STATUSCODE_GOOD;
    }
}

static const UA_DataValue*
getDataValue_sqliteHDB(UA_Server *server,
    void *hdbContext,
    const UA_NodeId *sessionId,
    void *sessionContext,
    const UA_NodeId *nodeId,
	size_t index)
{
    struct context_sqlite* context = (struct context_sqlite*)hdbContext;

    return NULL;
}


static UA_Boolean
boundSupported_sqliteHDB(UA_Server *server,
    void *hdbContext,
    const UA_NodeId *sessionId,
    void *sessionContext,
    const UA_NodeId *nodeId)
{
	return false; // We don't support returning bounds in this demo
}


static UA_Boolean
timestampsToReturnSupported_sqliteHDB(UA_Server *server,
    void *hdbContext,
    const UA_NodeId *sessionId,
    void *sessionContext,
    const UA_NodeId *nodeId,
    const UA_TimestampsToReturn timestampsToReturn)
{
    return true;
}


UA_HistoryDataBackend
UA_HistoryDataBackend_sqlite(const char* filename)
{
    UA_HistoryDataBackend result;
    memset(&result, 0, sizeof(UA_HistoryDataBackend));
    result.serverSetHistoryData = &serverSetHistoryData_sqliteHDB;
    result.resultSize = &resultSize_sqliteHDB;
    result.getEnd = &getEnd_sqliteHDB;
    result.lastIndex = &lastIndex_sqliteHDB;
    result.firstIndex = &firstIndex_sqliteHDB;
    result.getDateTimeMatch = &getDateTimeMatch_sqliteHDB;
    result.copyDataValues = &copyDataValues_sqliteHDB;
    result.getDataValue = &getDataValue_sqliteHDB;
    result.boundSupported = &boundSupported_sqliteHDB;
    result.timestampsToReturnSupported = &timestampsToReturnSupported_sqliteHDB;
    result.deleteMembers = NULL; // We don't support deleting in this demo
    result.getHistoryData = NULL; // We don't support the high level API in this demo
    result.context = generateContext_sqlite(filename);
    return result;
}


#endif
