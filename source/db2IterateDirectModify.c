#include <postgres.h>
#include "db2_fdw.h"
#include "DB2FdwDirectModifyState.h"

TupleTableSlot* db2IterateDirectModify(ForeignScanState *node);

/* postgresIterateDirectModify
 * Execute a direct foreign table modification
 */
TupleTableSlot* db2IterateDirectModify(ForeignScanState *node) {
	DB2FdwDirectModifyState* 	dmstate = (DB2FdwDirectModifyState*) node->fdw_state;
	EState*										estate 	= node->ss.ps.state;
	ResultRelInfo*						rtinfo 	= node->resultRelInfo;
	TupleTableSlot*						slot		= NULL;

	// nachfolgende Werte in ParamDesc Liste zusammenfassen
	int												numParams 		= dmstate->numParams;
	const char**							values 				= dmstate->param_values;
	FmgrInfo*									param_flinfo 	= dmstate->param_flinfo;
	List*											param_exps   	= dmstate->param_exprs;
	
	// call db2ExecForeignDirectUpdate() similar to db2ExecForeignInsert()
	return slot;
}
