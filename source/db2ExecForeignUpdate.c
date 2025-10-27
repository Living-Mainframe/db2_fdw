#include <postgres.h>
#include <nodes/makefuncs.h>
#include <utils/memutils.h>
#if PG_VERSION_NUM < 120000
#include <nodes/relation.h>
#include <optimizer/var.h>
#include <utils/tqual.h>
#else
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#endif
//#include "db2_pg.h"
#include "db2_fdw.h"
#include "ParamDesc.h"
#include "DB2FdwState.h"

/** external variables */
extern bool dml_in_transaction;

/** external prototypes */
extern int             db2ExecuteQuery           (DB2Session* session, const DB2Table* db2Table, ParamDesc* paramList);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2Debug2                 (const char* message, ...);
#ifdef WRITE_API
extern void            setModifyParameters       (ParamDesc* paramList, TupleTableSlot* newslot, TupleTableSlot* oldslot, DB2Table* db2Table, DB2Session* session);
#endif
extern void            convertTuple              (DB2FdwState* fdw_state, Datum* values, bool* nulls, bool trunc_lob) ;

/** local prototypes */
TupleTableSlot* db2ExecForeignUpdate      (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot);

/** db2ExecForeignUpdate
 *   Set the parameter values from the slots and execute the UPDATE statement.
 *   Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot* db2ExecForeignUpdate (EState* estate, ResultRelInfo* rinfo, TupleTableSlot* slot, TupleTableSlot* planSlot) {
  DB2FdwState*  fdw_state = (DB2FdwState*) rinfo->ri_FdwState;
  int           rows      = 0;
  MemoryContext oldcontext;

  db2Debug1("> db2ExecForeignUpdate");
  db2Debug2("  relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));

  ++fdw_state->rowcount;
  dml_in_transaction = true;

  MemoryContextReset (fdw_state->temp_cxt);
  oldcontext = MemoryContextSwitchTo (fdw_state->temp_cxt);

  /* extract the values from the slot and store them in the parameters */
  setModifyParameters (fdw_state->paramList, slot, planSlot, fdw_state->db2Table, fdw_state->session);

  /* execute the UPDATE statement and store RETURNING values in db2Table's columns */
  rows = db2ExecuteQuery (fdw_state->session, fdw_state->db2Table, fdw_state->paramList);

  if (rows != 1)
    ereport ( ERROR
            , ( errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION)
              , errmsg ("UPDATE on DB2 table changed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount)
              , errhint ("This probably means that you did not set the \"key\" option on all primary key columns.")
              )
            );

  MemoryContextSwitchTo (oldcontext);

  /* empty the result slot */
  ExecClearTuple (slot);

  /* convert result for RETURNING to arrays of values and null indicators */
  convertTuple (fdw_state, slot->tts_values, slot->tts_isnull, false);

  /* store the virtual tuple */
  ExecStoreVirtualTuple (slot);

  db2Debug1("< db2ExecForeignUpdate");
  return slot;
}

