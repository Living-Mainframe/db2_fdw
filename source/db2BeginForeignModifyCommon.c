#include <postgres.h>
#include <commands/explain.h>
#include <commands/vacuum.h>
#include <utils/builtins.h>
#include <utils/syscache.h>
#if PG_VERSION_NUM < 120000
#include <nodes/relation.h>
#include <optimizer/var.h>
#include <utils/tqual.h>
#else
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#endif
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external variables */
extern regproc* output_funcs;

/** external prototypes */
extern DB2Session*     db2GetSession             (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern void            db2PrepareQuery           (DB2Session* session, const char* query, DB2Table* db2Table, unsigned int prefetch);
extern void            db2Debug1                 (const char* message, ...);

/** local prototypes */
void db2BeginForeignModifyCommon(ModifyTableState* mtstate, ResultRelInfo* rinfo, DB2FdwState* fdw_state, Plan* subplan);

void db2BeginForeignModifyCommon(ModifyTableState* mtstate, ResultRelInfo* rinfo, DB2FdwState* fdw_state, Plan* subplan) {
  EState    *estate = mtstate->ps.state;
  ParamDesc *param  = NULL;
  HeapTuple  tuple;
  int        i;

  db2Debug1("> db2BeginForeignModifyCommon");
  rinfo->ri_FdwState = fdw_state;

  /* connect to DB2 database */
  fdw_state->session = db2GetSession(fdw_state->dbserver, fdw_state->user, fdw_state->password, fdw_state->jwt_token, fdw_state->nls_lang, GetCurrentTransactionNestLevel());
  db2PrepareQuery(fdw_state->session, fdw_state->query, fdw_state->db2Table,0);

  /* get the type output functions for the parameters */
  output_funcs = (regproc*) palloc0(fdw_state->db2Table->ncols * sizeof(regproc *));
  for (param = fdw_state->paramList; param != NULL; param = param->next) {
    /* ignore output parameters */
    if (param->bindType == BIND_OUTPUT)
      continue;
    
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(fdw_state->db2Table->cols[param->colnum]->pgtype));
    if (!HeapTupleIsValid(tuple))
      elog(ERROR, "cache lookup failed for type %u", fdw_state->db2Table->cols[param->colnum]->pgtype);
    output_funcs[param->colnum] = ((Form_pg_type) GETSTRUCT(tuple))->typoutput;
    ReleaseSysCache(tuple);
  }

  /* primary-key junk attrs are only needed for UPDATE/DELETE */
  if (subplan != NULL) {
    for (i = 0; i < fdw_state->db2Table->ncols; ++i) {
      if (!fdw_state->db2Table->cols[i]->colPrimKeyPart)
        continue;
      fdw_state->db2Table->cols[i]->pkey = ExecFindJunkAttributeInTlist(subplan->targetlist, fdw_state->db2Table->cols[i]->pgname);
    }
  }

  /* create a memory context for short-lived memory */
  fdw_state->temp_cxt = AllocSetContextCreate(estate->es_query_cxt, "db2_fdw temporary data", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
  db2Debug1("< db2BeginForeignModifyCommon");
}
