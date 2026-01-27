#include <postgres.h>
#include <utils/builtins.h>
#include <nodes/makefuncs.h>
#include <nodes/pathnodes.h>

//#include <parser/parse_relation.h>
//#include <parser/parsetree.h>
//#include <utils/builtins.h>
//#include <nodes/pathnodes.h>
//#include <optimizer/optimizer.h>
//#include <access/heapam.h>


#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2Debug1           (const char* message, ...);
extern void         db2Debug2           (const char* message, ...);
extern void         db2Debug4           (const char* message, ...);
extern void*        db2alloc            (const char* type, size_t size);
extern char*        c2name              (short fcType);

/** local prototypes */
       DB2FdwState* deserializePlanData (List* list);
       List*        serializePlanData   (DB2FdwState* fdwState);

static char*        deserializeString   (Const* constant);
static long         deserializeLong     (Const* constant);
static Const*       serializeString     (const char* s);
static Const*       serializeLong       (long i);

/** deserializePlanData
 *   Extract the data structures from a List created by serializePlanData.
 */
DB2FdwState* deserializePlanData (List* list) {
  DB2FdwState* state  = db2alloc ("DB2FdwState", sizeof (DB2FdwState));
  int          idx    = 0; 
  int          i      = 0; 
  int          len    = 0;
  ParamDesc*   param  = NULL;

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

  state->retrieved_attr = (List *) list_nth(list, idx++);
  /* dbserver */
  state->dbserver = deserializeString(list_nth(list, idx++));
  /* user */
  state->user = deserializeString(list_nth(list, idx++));
  /* password */
  state->password = deserializeString(list_nth(list, idx++));
  /* nls_lang */
  state->nls_lang = deserializeString(list_nth(list, idx++));
  /* query */
  state->query = deserializeString(list_nth(list, idx++));
  /* DB2 prefetch count */
  state->prefetch = (unsigned long) DatumGetInt32 (((Const*)list_nth(list, idx++))->constvalue);
  /* DB2 fetch_size */
  state->fetch_size = (unsigned long) DatumGetInt32 (((Const*)list_nth(list, idx++))->constvalue);
  /* relation_name */
  state->relation_name  = deserializeString(list_nth(list, idx++));
  db2Debug2("  state->relation_name: '%s'",state->relation_name);
  /* table data */
  state->db2Table = (DB2Table*) db2alloc ("state->db2Table", sizeof (struct db2Table));
  state->db2Table->name = deserializeString(list_nth(list, idx++));
  db2Debug2("  state->db2Table->name: '%s'",state->db2Table->name);
  state->db2Table->pgname = deserializeString(list_nth(list, idx++));
  db2Debug2("  state->db2Table->pgname: '%s'",state->db2Table->pgname);
  state->db2Table->batchsz = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
  db2Debug2("  state->db2Table->batchsz: %d",state->db2Table->batchsz);
  state->db2Table->ncols = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
  db2Debug2("  state->db2Table->ncols: %d",state->db2Table->ncols);
  state->db2Table->npgcols = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
  db2Debug2("  state->db2Table->npgcols: %d",state->db2Table->npgcols);
  state->db2Table->cols = (DB2Column**) db2alloc ("state->db2Table->cols", sizeof (DB2Column*) * state->db2Table->ncols);

  /* loop columns */
  for (i = 0; i < state->db2Table->ncols; ++i) {
    state->db2Table->cols[i]           = (DB2Column *) db2alloc ("state->db2Table->cols[i]", sizeof (DB2Column));
    state->db2Table->cols[i]->colName  = deserializeString(list_nth(list, idx++));
    db2Debug2("  state->db2Table->cols[%d]->colName: '%s'",i,state->db2Table->cols[i]->colName);
    state->db2Table->cols[i]->colType  = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colType: %d (%s)",i,state->db2Table->cols[i]->colType,c2name(state->db2Table->cols[i]->colType));
    state->db2Table->cols[i]->colSize  = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colSize: %lld",i,state->db2Table->cols[i]->colSize);
    state->db2Table->cols[i]->colScale = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colScale: %d",i,state->db2Table->cols[i]->colScale);
    state->db2Table->cols[i]->colNulls = (short) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->colNulls: %d",i,state->db2Table->cols[i]->colNulls);
    state->db2Table->cols[i]->colChars = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colChars: %lld",i,state->db2Table->cols[i]->colChars);
    state->db2Table->cols[i]->colBytes = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colBytes: %lld",i,state->db2Table->cols[i]->colBytes);
    state->db2Table->cols[i]->colPrimKeyPart = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colPrimKeyPart: %lld",i,state->db2Table->cols[i]->colPrimKeyPart);
    state->db2Table->cols[i]->colCodepage = (size_t) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%lld]->colCodepaget: %lld",i,state->db2Table->cols[i]->colCodepage);
    state->db2Table->cols[i]->pgname   = deserializeString(list_nth(list, idx++));
    db2Debug2("  state->db2Table->cols[%d]->pgname: '%s'",i,state->db2Table->cols[i]->pgname);
    state->db2Table->cols[i]->pgattnum = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgattnum: %d",i,state->db2Table->cols[i]->pgattnum);
    state->db2Table->cols[i]->pgtype   = DatumGetObjectId(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgtype: %d",i,state->db2Table->cols[i]->pgtype);
    state->db2Table->cols[i]->pgtypmod = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pgtypmod: %d",i,state->db2Table->cols[i]->pgtypmod);
    state->db2Table->cols[i]->used     = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->used: %d",i,state->db2Table->cols[i]->used);
    state->db2Table->cols[i]->pkey     = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    db2Debug2("  state->db2Table->cols[%d]->pkey: %d",i,state->db2Table->cols[i]->pkey);
    state->db2Table->cols[i]->val_size = deserializeLong(list_nth(list, idx++));
    db2Debug2("  state->db2Table->cols[%d]->val_size: %ld",i,state->db2Table->cols[i]->val_size);
    state->db2Table->cols[i]->noencerr = deserializeLong(list_nth(list, idx++));
    db2Debug2("  state->db2Table->cols[%d]->noencerr: %d",i,state->db2Table->cols[i]->noencerr);
    /* allocate memory for the result value only when the column is used in query */
    state->db2Table->cols[i]->val      = (state->db2Table->cols[i]->used == 1) ? (char*) db2alloc ("state->db2Table->cols[i]->val", state->db2Table->cols[i]->val_size + 1) : NULL;
    db2Debug2("  state->db2Table->cols[%d]->val: %x",i,state->db2Table->cols[i]->val);
    state->db2Table->cols[i]->val_len  = 0;
    db2Debug2("  state->db2Table->cols[%d]->val_len: %d",i,state->db2Table->cols[i]->val_len);
    state->db2Table->cols[i]->val_null = 1;
    db2Debug2("  state->db2Table->cols[%d]->val_null: %d",i,state->db2Table->cols[i]->val_null);
  }

  /* length of parameter list */
  len  = (int) DatumGetInt32 (((Const*)list_nth(list, idx++))->constvalue);

  /* parameter table entries */
  state->paramList = NULL;
  for (i = 0; i < len; ++i) {
    param            = (ParamDesc*) db2alloc ("state->parmList->next", sizeof (ParamDesc));
    param->type      = DatumGetObjectId(((Const*)list_nth(list, idx++))->constvalue);
    param->bindType  = (db2BindType) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    if (param->bindType == BIND_OUTPUT)
      param->value   = (void *) 42;	/* something != NULL */
    else
      param->value   = NULL;
    param->node      = NULL;
    param->colnum    = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    param->txts      = (int) DatumGetInt32(((Const*)list_nth(list, idx++))->constvalue);
    param->next      = state->paramList;
    state->paramList = param;
  }
  db2Debug1("< deserializePlanData - returns: %x", state);
  return state;
}

/** deserializeString
 *   Extracts a string from a Const, returns a deep copy.
 */
static char* deserializeString (Const* constant) {
  char* result = NULL;
  db2Debug4("> deserializeString");
  if (!constant->constisnull)
    result = text_to_cstring (DatumGetTextP (constant->constvalue));
  db2Debug4("< deserializeString: '%s'", result);
  return result;
}

/** deserializeLong
 *   Extracts a long integer from a Const.
 */
static long deserializeLong (Const* constant) {
  long result = 0L;
  db2Debug4("> deserializeLong");
  result =  (sizeof (long) <= 4) ? (long) DatumGetInt32 (constant->constvalue)
                                 : (long) DatumGetInt64 (constant->constvalue);
  db2Debug4("< deserializeLong - returns: %ld", result);
  return result;
}

/** serializePlanData
 *   Create a List representation of plan data that copyObject can copy.
 *   This List can be parsed by deserializePlanData.
 */
List* serializePlanData (DB2FdwState* fdwState) {
  List*      result   = NIL;
  int        idxCol   = 0;
  int        lenParam = 0;
  ParamDesc* param    = NULL;

  db2Debug1("> serializePlanData");
  result = list_make1(fdwState->retrieved_attr);
  /* dbserver */
  result = lappend (result, serializeString (fdwState->dbserver));
  /* user name */
  result = lappend (result, serializeString (fdwState->user));
  /* password */
  result = lappend (result, serializeString (fdwState->password));
  /* nls_lang */
  result = lappend (result, serializeString (fdwState->nls_lang));
  /* query */
  result = lappend (result, serializeString (fdwState->query));
  /* DB2 prefetch count */
  result = lappend (result, serializeLong (fdwState->prefetch));
  /* DB2 fetchsize count */
  result = lappend (result, serializeLong (fdwState->fetch_size));
  /* relation_name */
  result = lappend (result, serializeString (fdwState->relation_name));
  /* DB2 table name */
  result = lappend (result, serializeString (fdwState->db2Table->name));
  /* PostgreSQL table name */
  result = lappend (result, serializeString (fdwState->db2Table->pgname));
  /* batch size in DB2 table */
  result = lappend (result, serializeInt (fdwState->db2Table->batchsz));
  /* number of columns in DB2 table */
  result = lappend (result, serializeInt (fdwState->db2Table->ncols));
  /* number of columns in PostgreSQL table */
  result = lappend (result, serializeInt (fdwState->db2Table->npgcols));
  /* column data */
  for (idxCol = 0; idxCol < fdwState->db2Table->ncols; ++idxCol) {
    result = lappend (result, serializeString (fdwState->db2Table->cols[idxCol]->colName));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colType));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colSize));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colScale));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colNulls));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colChars));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colBytes));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colPrimKeyPart));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->colCodepage));
    result = lappend (result, serializeString (fdwState->db2Table->cols[idxCol]->pgname));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->pgattnum));
    result = lappend (result, serializeOid    (fdwState->db2Table->cols[idxCol]->pgtype));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->pgtypmod));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->used));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->pkey));
    result = lappend (result, serializeLong   (fdwState->db2Table->cols[idxCol]->val_size));
    result = lappend (result, serializeInt    (fdwState->db2Table->cols[idxCol]->noencerr));
    /* don't serialize val, val_len, val_null and varno */
  }

  /* find length of parameter list */
  for (param = fdwState->paramList; param; param = param->next) {
    ++lenParam;
  }
  /* serialize length */
  result = lappend (result, serializeInt (lenParam));
  /* parameter list entries */
  for (param = fdwState->paramList; param; param = param->next) {
    result = lappend (result, serializeOid (param->type));
    result = lappend (result, serializeInt ((int) param->bindType));
    result = lappend (result, serializeInt ((int) param->colnum));
    result = lappend (result, serializeInt ((int) param->txts));
    /* don't serialize value and node */
  }
  /* don't serialize params, startup_cost, total_cost, rowcount, columnindex, temp_cxt, order_clause and where_clause */
  db2Debug1("< serializePlanData - returns: %x",result);
  return result;
}

/** serializeString
 *   Create a Const that contains the string.
 */
static Const* serializeString (const char* s) {
  Const* result = NULL;
  db2Debug1("> serializeString");
  result = (s == NULL) ? makeNullConst (TEXTOID, -1, InvalidOid) 
                       : makeConst (TEXTOID, -1, InvalidOid, -1, PointerGetDatum (cstring_to_text (s)), false, false);
  db2Debug1("< serializeString - returns: %x",result);
  return result;
}

/** serializeLong
 *   Create a Const that contains the long integer.
 */
static Const* serializeLong (long i) {
  Const* result = NULL;
  db2Debug1("> serializeLong");
  if (sizeof (long) <= 4)
    result = makeConst (INT4OID, -1, InvalidOid, 4, Int32GetDatum ((int32) i), false, true);
  else
    result = makeConst (INT4OID, -1, InvalidOid, 8, Int64GetDatum ((int64) i), false,
#ifdef USE_FLOAT8_BYVAL
      true
#else
      false
#endif /* USE_FLOAT8_BYVAL */
      );
  db2Debug1("< serializeLong - returns: %x",result);
  return result;
}
