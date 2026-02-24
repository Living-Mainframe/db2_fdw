#include <postgres.h>
#include <commands/explain.h>
#if PG_VERSION_NUM >= 180000
#include <commands/explain_format.h>
#endif
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2Entry                  (int level, const char* message, ...);
extern void         db2Exit                   (int level, const char* message, ...);
extern void         db2Debug                  (int level, const char* message, ...);

/** local prototypes */
void db2ExplainForeignModify (ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, struct ExplainState* es);

/* db2ExplainForeignModify
 * Show the DB2 DML statement.
 * Nothing special is done for VERBOSE because the query plan is likely trivial.
 */
void db2ExplainForeignModify (ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, struct ExplainState* es) {
  DB2FdwState* fdw_state = (DB2FdwState*) rinfo->ri_FdwState;
  db2Entry(1,"> db2ExplainForeignModify.c::db2ExplainForeignModify");
  db2Debug(2,"relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));
  /* show query */
  ExplainPropertyText ("DB2 statement", fdw_state->query, es);
  db2Exit(1,"< db2ExplainForeignModify.c::db2ExplainForeignModify");
}

