#include <signal.h>
#include "open62541.h"
#include "SQLiteBackend.h"


static UA_Boolean running = true;

static void stopHandler(int sign) 
{
    (void)sign;
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
    running = false;
}


int main(void) 
{
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);


    UA_Server *server = UA_Server_new();
    UA_ServerConfig *config = UA_Server_getConfig(server);
    UA_ServerConfig_setDefault(config);


    /*
    * We need a gathering for the plugin to constuct.
    * The UA_HistoryDataGathering is responsible to collect data and store it to the database.
    * We will use this gathering for one node, only. initialNodeIdStoreSize = 1
    * The store will grow if you register more than one node, but this is expensive.
    */
    UA_HistoryDataGathering gathering = UA_HistoryDataGathering_Default(1);

    /*
    * We set the responsible plugin in the configuration.
    * UA_HistoryDatabase is the main plugin which handles the historical data service.
    */
    config->historyDatabase = UA_HistoryDatabase_default(gathering);


    /* Define the attribute of the uint32 variable node */
    UA_VariableAttributes attr = UA_VariableAttributes_default;
    UA_Double myDouble = 17.2;
    UA_Variant_setScalar(&attr.value, &myDouble, &UA_TYPES[UA_TYPES_DOUBLE]);
    attr.description = UA_LOCALIZEDTEXT("en-US", "myDoubleValue");
    attr.displayName = UA_LOCALIZEDTEXT("en-US", "myDoubleValue");
    attr.dataType = UA_TYPES[UA_TYPES_DOUBLE].typeId;

    /*
    * We set the access level to also support history read
    * This is what will be reported to clients
    */
    attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE | UA_ACCESSLEVELMASK_HISTORYREAD;

    /*
    * We also set this node to historizing, so the server internals also know from it.
    */
    attr.historizing = true;

    /* Add the variable node to the information model */
    UA_NodeId doubleNodeId = UA_NODEID_STRING(1, "myDoubleValue");
    UA_QualifiedName doubleName = UA_QUALIFIEDNAME(1, "myDoubleValue");
    UA_NodeId parentNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
    UA_NodeId parentReferenceNodeId = UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES);
    UA_NodeId outNodeId;
    UA_NodeId_init(&outNodeId);
    UA_StatusCode retval = UA_Server_addVariableNode(server,
        doubleNodeId,
        parentNodeId,
        parentReferenceNodeId,
        doubleName,
        UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
        attr,
        NULL,
        &outNodeId);

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "UA_Server_addVariableNode %s", UA_StatusCode_name(retval));

    /*
    * Now we define the settings for our node
    */
    UA_HistorizingNodeIdSettings setting;


    setting.historizingBackend = UA_HistoryDataBackend_sqlite("database.sqlite");

    /*
    * We want the server to serve a maximum of 100 values per request.
    * This value depend on the plattform you are running the server.
    * A big server can serve more values, smaller ones less.
    */
    setting.maxHistoryDataResponseSize = 100;


    setting.historizingUpdateStrategy = UA_HISTORIZINGUPDATESTRATEGY_VALUESET;

    /*
    * At the end we register the node for gathering data in the database.
    */
    retval = gathering.registerNodeId(server, gathering.context, &outNodeId, setting);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "registerNodeId %s", UA_StatusCode_name(retval));


    retval = UA_Server_run(server, &running);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "UA_Server_run %s", UA_StatusCode_name(retval));


    UA_Server_delete(server);

    return (int)retval;

}
