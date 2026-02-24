#include <postgres.h>
#include "db2_fdw.h"
#include "DB2FdwDirectModifyState.h"

/** external prototypes */
extern void         db2Entry                  (int level, const char* message, ...);
extern void         db2Exit                   (int level, const char* message, ...);
extern void         db2Debug                  (int level, const char* message, ...);

/** local prototypes */
TupleTableSlot* db2IterateDirectModify(ForeignScanState *node);

/* postgresIterateDirectModify
 * Execute a direct foreign table modification
 */
TupleTableSlot* db2IterateDirectModify(ForeignScanState *node) {
  DB2FdwDirectModifyState* 	dmstate = (DB2FdwDirectModifyState*) node->fdw_state;
  EState*         estate  = node->ss.ps.state;
  ResultRelInfo*  rtinfo  = node->resultRelInfo;
  TupleTableSlot* slot    = NULL;

  // nachfolgende Werte in ParamDesc Liste zusammenfassen
  int             numParams     = dmstate->numParams;
  const char**    values        = dmstate->param_values;
  FmgrInfo*       param_flinfo  = dmstate->param_flinfo;
  List*           param_exps    = dmstate->param_exprs;
  
  // call db2ExecForeignDirectUpdate() similar to db2ExecForeignInsert()
  db2Entry(1,"> db2IterateDirectModify.c::db2IterateDirectModify");
  db2Exit(1,"> db2IterateDirectModify.c::db2IterateDirectModify : %x", slot);
  return slot;
}
