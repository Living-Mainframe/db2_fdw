#include <postgres.h>
#include "db2_fdw.h"
#include "DB2FdwDirectModifyState.h"

/** external prototypes */
extern int          db2IsStatementOpen        (DB2Session* session);
extern void         db2PrepareQuery           (DB2Session* session, const char *query, DB2ResultColumn* resultList, unsigned long prefetch, int fetchsize);
extern int          db2ExecuteQuery           (DB2Session* session, ParamDesc* paramList);
extern int          db2FetchNext              (DB2Session* session);
extern void         db2CloseStatement         (DB2Session* session);

/** local prototypes */
TupleTableSlot* db2IterateDirectModify(ForeignScanState *node);

/* postgresIterateDirectModify
 * Execute a direct foreign table modification
 */
TupleTableSlot* db2IterateDirectModify(ForeignScanState *node) {
  DB2FdwDirectModifyState* 	dmstate     = (DB2FdwDirectModifyState*) node->fdw_state;
  EState*                   estate      = node->ss.ps.state;
  ResultRelInfo*            rtinfo      = node->resultRelInfo;
  int                       have_result = 0;
  TupleTableSlot*           slot        = NULL;

  // nachfolgende Werte in ParamDesc Liste zusammenfassen
  //int             numParams     = dmstate->numParams;
  //const char**    values        = dmstate->param_values;
  //FmgrInfo*       param_flinfo  = dmstate->param_flinfo;
  //List*           param_exps    = dmstate->param_exprs;
  
  db2Entry1();
  // We should have a valid session.
  // It was created during db2GetFdwDirectModifyState during db2BeginDirectModify

  if (db2IsStatementOpen(dmstate->session)) {

  } else {
    db2PrepareQuery(dmstate->session, dmstate->query, NULL, dmstate->prefetch, dmstate->fetch_size);
    have_result = db2ExecuteQuery (dmstate->session, dmstate->paramList);
    db2Debug2(" %d rows affected by this query", have_result);
  }

  /* initialize virtual tuple */
//  ExecClearTuple (slot);
//  if (have_result) {
    /* increase row count */
//    ++dmstate->rowcount;
    /* convert result to arrays of values and null indicators */
//    db2Debug2("slot->tts_tupleDescriptor->natts: %d",slot->tts_tupleDescriptor->natts);
//    convertTuple (dmstate, slot->tts_tupleDescriptor->natts, slot->tts_values, slot->tts_isnull, false);
    /* store the virtual tuple */
//    ExecStoreVirtualTuple (slot);
//  } else {
    /* close the statement */
    db2CloseStatement (dmstate->session);
//  }
  db2Exit1(": %x", slot);
  return slot;
}
