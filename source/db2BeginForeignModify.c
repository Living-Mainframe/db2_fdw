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
//#include "db2_pg.h"
#include "db2_fdw.h"
#include "ParamDesc.h"
#include "DB2FdwState.h"

/** external variables */
extern regproc* output_funcs;

/** external prototypes */
extern DB2Session*     db2GetSession             (const char* connectstring, char* user, char* password, const char* nls_lang, int curlevel);
extern void            db2PrepareQuery           (DB2Session* session, const char* query, DB2Table* db2Table, unsigned int prefetch);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2Debug2                 (const char* message, ...);
extern char*           c2name                    (short fcType);

/** local prototypes */
void         db2BeginForeignModify(ModifyTableState* mtstate, ResultRelInfo* rinfo, List* fdw_private, int subplan_index, int eflags);
DB2FdwState* deserializePlanData  (List* list);
char*        deserializeString    (Const* constant);
long         deserializeLong      (Const* constant);

/** db2BeginForeignModify
 *   Prepare everything for the DML query:
 *   The SQL statement is prepared, the type output functions for
 *   the parameters are fetched, and the column numbers of the
 *   resjunk attributes are stored in the "pkey" field.
 */
void db2BeginForeignModify (ModifyTableState * mtstate, ResultRelInfo * rinfo, List * fdw_private, int subplan_index, int eflags) {
  DB2FdwState* fdw_state = deserializePlanData (fdw_private);
  EState*      estate    = mtstate->ps.state;
  ParamDesc*   param     = NULL;
  HeapTuple    tuple;
  int          i         = 0;
  #if PG_VERSION_NUM < 140000
  Plan*        subplan   = mtstate->mt_plans[subplan_index]->plan;
  #else
  Plan*        subplan   = outerPlanState(mtstate)->plan;
  #endif

  db2Debug1("> db2BeginForeignModify");
  db2Debug2("  relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));
  rinfo->ri_FdwState = fdw_state;

  /* connect to DB2 database */
  fdw_state->session = db2GetSession (fdw_state->dbserver, fdw_state->user, fdw_state->password, fdw_state->nls_lang, GetCurrentTransactionNestLevel ());
  db2PrepareQuery (fdw_state->session, fdw_state->query, fdw_state->db2Table, 0);

  /* get the type output functions for the parameters */
  output_funcs = (regproc *) palloc0 (fdw_state->db2Table->ncols * sizeof (regproc *));
  for (param = fdw_state->paramList; param != NULL; param = param->next) {
    /* ignore output parameters */
    if (param->bindType == BIND_OUTPUT)
      continue;

    tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (fdw_state->db2Table->cols[param->colnum]->pgtype));
    if (!HeapTupleIsValid (tuple))
      elog (ERROR, "cache lookup failed for type %u", fdw_state->db2Table->cols[param->colnum]->pgtype);
    output_funcs[param->colnum] = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
    ReleaseSysCache (tuple);
  }

  /* loop through table columns */
  for (i = 0; i < fdw_state->db2Table->ncols; ++i) {
    if (!fdw_state->db2Table->cols[i]->colPrimKeyPart)
      continue;
    /* for primary key columns, get the resjunk attribute number and store it in "pkey" */
    fdw_state->db2Table->cols[i]->pkey = ExecFindJunkAttributeInTlist (subplan->targetlist, fdw_state->db2Table->cols[i]->pgname);
  }

  /* create a memory context for short-lived memory */
  fdw_state->temp_cxt = AllocSetContextCreate (estate->es_query_cxt, "db2_fdw temporary data", ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);
  db2Debug1("< db2BeginForeignModify");
}

/** deserializePlanData
 *   Extract the data structures from a List created by serializePlanData.
 */
DB2FdwState* deserializePlanData (List* list) {
  DB2FdwState* state = palloc (sizeof (DB2FdwState));
  ListCell*    cell  = list_head (list);
  int          i, 
               len;
  ParamDesc*   param;

  db2Debug1("> deserializePlanData");
  /* session will be set upon connect */
  state->session      = NULL;
  /* these fields are not needed during execution */
  state->startup_cost = 0;
  state->total_cost   = 0;
  /* these are not serialized */
  state->rowcount     = 0;
  state->columnindex  = 0;
  state->params       = NULL;
  state->temp_cxt     = NULL;
  state->order_clause = NULL;

  /* dbserver */
  state->dbserver = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* user */
  state->user = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* password */
  state->password = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* nls_lang */
  state->nls_lang = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* query */
  state->query = deserializeString (lfirst (cell));
  cell = list_next (list,cell);

  /* DB2 prefetch count */
  state->prefetch = (unsigned int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
  cell = list_next (list,cell);

  /* table data */
  state->db2Table = (DB2Table*) palloc (sizeof (struct db2Table));
  state->db2Table->name = deserializeString (lfirst (cell));
  db2Debug2("  state->db2Table->name: '%s'",state->db2Table->name);
  cell = list_next (list,cell);
  state->db2Table->pgname = deserializeString (lfirst (cell));
  db2Debug2("  state->db2Table->pgname: '%s'",state->db2Table->pgname);
  cell = list_next (list,cell);
  state->db2Table->ncols = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
  db2Debug2("  state->db2Table->ncols: %d",state->db2Table->ncols);
  cell = list_next (list,cell);
  state->db2Table->npgcols = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
  db2Debug2("  state->db2Table->npgcols: %d",state->db2Table->npgcols);
  cell = list_next (list,cell);
  state->db2Table->cols = (DB2Column**) palloc (sizeof (DB2Column*) * state->db2Table->ncols);

  /* loop columns */
  for (i = 0; i < state->db2Table->ncols; ++i) {
    state->db2Table->cols[i]           = (DB2Column *) palloc (sizeof (DB2Column));
    state->db2Table->cols[i]->colName  = deserializeString (lfirst (cell));
    db2Debug2("  state->db2Table->cols[%d]->colName: '%s'",i,state->db2Table->cols[i]->colName);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colType  = (short) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colType: %d (%s)",i,state->db2Table->cols[i]->colType,c2name(state->db2Table->cols[i]->colType));
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colSize  = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colSize: %lld",i,state->db2Table->cols[i]->colSize);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colScale = (short) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colScale: %d",i,state->db2Table->cols[i]->colScale);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colNulls = (short) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colNulls: %d",i,state->db2Table->cols[i]->colNulls);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colChars = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colChars: %lld",i,state->db2Table->cols[i]->colChars);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colBytes = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colBytes: %lld",i,state->db2Table->cols[i]->colBytes);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colPrimKeyPart = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colPrimKeyPart: %lld",i,state->db2Table->cols[i]->colPrimKeyPart);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->colCodepage = (size_t) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colCodepaget: %lld",i,state->db2Table->cols[i]->colCodepage);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgname   = deserializeString (lfirst (cell));
    db2Debug2("  state->db2Table->cols[%d]->pgname: '%s'",i,state->db2Table->cols[i]->pgname);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgattnum = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgattnum: %d",i,state->db2Table->cols[i]->pgattnum);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgtype   = DatumGetObjectId (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgtype: %d",i,state->db2Table->cols[i]->pgtype);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pgtypmod = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgtypmod: %d",i,state->db2Table->cols[i]->pgtypmod);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->used     = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->used: %d",i,state->db2Table->cols[i]->used);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->pkey     = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pkey: %d",i,state->db2Table->cols[i]->pkey);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->val_size = deserializeLong (lfirst (cell));
    db2Debug2("  state->db2Table->cols[%d]->val_size: %ld",i,state->db2Table->cols[i]->val_size);
    cell = list_next (list,cell);
    state->db2Table->cols[i]->noencerr = deserializeLong (lfirst (cell));
    db2Debug2("  state->db2Table->cols[%d]->noencerr: %d",i,state->db2Table->cols[i]->noencerr);
    cell = list_next (list,cell);
    /* allocate memory for the result value only when the column is used in query */
    state->db2Table->cols[i]->val      = (state->db2Table->cols[i]->used == 1) ? (char*) palloc (state->db2Table->cols[i]->val_size + 1) : NULL;
    db2Debug2("  state->db2Table->cols[%d]->val: %x",i,state->db2Table->cols[i]->val);
    state->db2Table->cols[i]->val_len  = 0;
    db2Debug2("  state->db2Table->cols[%d]->val_len: %d",i,state->db2Table->cols[i]->val_len);
    state->db2Table->cols[i]->val_null = 1;
    db2Debug2("  state->db2Table->cols[%d]->val_null: %d",i,state->db2Table->cols[i]->val_null);
  }

  /* length of parameter list */
  len  = (int) DatumGetInt32 (((Const*) lfirst (cell))->constvalue);
  cell = list_next (list,cell);

  /* parameter table entries */
  state->paramList = NULL;
  for (i = 0; i < len; ++i) {
    param            = (ParamDesc*) palloc (sizeof (ParamDesc));
    param->type      = DatumGetObjectId (((Const *) lfirst (cell))->constvalue);
    cell             = list_next (list,cell);
    param->bindType  = (db2BindType) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell             = list_next (list,cell);
    if (param->bindType == BIND_OUTPUT)
      param->value   = (void *) 42;	/* something != NULL */
    else
      param->value   = NULL;
    param->node      = NULL;
    param->colnum    = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell             = list_next (list,cell);
    param->txts      = (int) DatumGetInt32 (((Const *) lfirst (cell))->constvalue);
    cell             = list_next (list,cell);
    param->next      = state->paramList;
    state->paramList = param;
  }

  db2Debug1("< deserializePlanData - returns: %x", state);
  return state;
}

/** deserializeString
 *   Extracts a string from a Const, returns a palloc'ed copy.
 */
char* deserializeString (Const* constant) {
  char* result = NULL;
  db2Debug1("> deserializeString");
  if (constant->constisnull)
    result = NULL;
  else
    result = text_to_cstring (DatumGetTextP (constant->constvalue));
  db2Debug1("< deserializeString: '%s'", result);
  return result;
}

/** deserializeLong
 *   Extracts a long integer from a Const.
 */
long deserializeLong (Const* constant) {
  long result = 0L;
  db2Debug1("> deserializeLong");
  result =  (sizeof (long) <= 4) ? (long) DatumGetInt32 (constant->constvalue)
                                 : (long) DatumGetInt64 (constant->constvalue);
  db2Debug1("< deserializeLong - returns: %ld", result);
  return result;
}
