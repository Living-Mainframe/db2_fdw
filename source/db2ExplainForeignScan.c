#include <postgres.h>
#include <commands/explain.h>
#if PG_VERSION_NUM >= 180000
#include <commands/explain_state.h>
#include <commands/explain_format.h>
#endif
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void*        db2alloc                  (const char* type, size_t size);
extern void         db2free                   (void* p);

/** local prototypes */
       void db2ExplainForeignScan(ForeignScanState* node, ExplainState* es);
static void db2Explain           (void* fdw, ExplainState* es);

/* db2ExplainForeignScan
 * Produce extra output for EXPLAIN:
 * the DB2 query and, if VERBOSE was given, the execution plan.
 */
void db2ExplainForeignScan (ForeignScanState* node, ExplainState* es) {
  DB2FdwState* fdw_state = (DB2FdwState*) node->fdw_state;
  db2Entry1();
  db2Debug2("db2_fdw: explain foreign table scan");
  ExplainPropertyText ("DB2 query", fdw_state->query, es);
  db2Explain (fdw_state, es);
  db2Exit1();
}

/* db2Explain */
static void db2Explain (void* fdw, ExplainState* es) {
  FILE*        fp;
  char         path[1035];
  char         execution_cmd[300];
  DB2FdwState* fdw_state = (DB2FdwState*) fdw;
  int          count     = 0;
  int          qlength   = strlen(fdw_state->query);
  char*        tempQuery = NULL;
  char*        src       = fdw_state->query;
  char*        dest      = NULL;

  db2Entry1();
  for (const char* p = src; *p; p++) {
    if (*p == '"') count++;
  }
  tempQuery = db2alloc("tempQuery", qlength+count+1);
  dest      = tempQuery;
  src       = fdw_state->query;
  while(*src){
    if (*src == '"') {
      *dest++ = '\\';
    }
    *dest++ = *(src);
    src++;
  }
  *dest = '\0';

  memset(execution_cmd,0x00,sizeof(execution_cmd));
  if (es->verbose) {
    if (strlen(fdw_state->user)){
      snprintf(execution_cmd,sizeof(execution_cmd),"db2expln -t -d %s -u %s %s -q \"%s\" ",fdw_state->dbserver,fdw_state->user,fdw_state->password,tempQuery);
    } else {
      snprintf(execution_cmd,sizeof(execution_cmd),"db2expln -t -d %s -q \"%s\" ",fdw_state->dbserver,tempQuery);
    }
  } else {
    if (strlen(fdw_state->user)){
      snprintf(execution_cmd,sizeof(execution_cmd),"db2expln -t -d %s -u %s %s -q \"%s\" |grep -E \"Estimated Cost|Estimated Cardinality\" ",fdw_state->dbserver,fdw_state->user,fdw_state->password,tempQuery);
    } else {
      snprintf(execution_cmd,sizeof(execution_cmd),"db2expln -t -d %s -q \"%s\" |grep -E \"Estimated Cost|Estimated Cardinality\" ",fdw_state->dbserver,tempQuery);
    }
  }
  db2Debug2("execution_cmd: '%s'",execution_cmd);
  /* Open the command for reading. */
  fp = popen(execution_cmd, "r");
  if (fp == NULL) {
    elog (ERROR, "db2_fdw: Failed to run command");
    exit(1);
  }

  /* Read the output a line at a time - output it. */
  while (fgets(path, sizeof(path)-1, fp) != NULL) {
    path[strlen (path) - 1] = '\0';
    ExplainPropertyText ("DB2 plan", path, es);
  }
  /* close */
  pclose(fp);
  db2free(tempQuery);
  db2Exit1();
}
