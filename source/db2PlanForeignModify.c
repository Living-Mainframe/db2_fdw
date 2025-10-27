#include <postgres.h>
#include <nodes/makefuncs.h>
#include <parser/parse_relation.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
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

/** external prototypes */
extern void*           db2Alloc                  (size_t size);
extern DB2FdwState*    db2GetFdwState            (Oid foreigntableid, double* sample_percent);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2Debug2                 (const char* message, ...);
extern void            db2Debug3                 (const char* message, ...);
extern short           c2dbType                  (short fcType);
extern void            appendAsType              (StringInfoData* dest, Oid type);

/** local prototypes */
List*        db2PlanForeignModify(PlannerInfo* root, ModifyTable* plan, Index resultRelation, int subplan_index);
#ifdef WRITE_API
DB2FdwState* copyPlanData        (DB2FdwState* orig);
void         addParam            (ParamDesc** paramList, Oid pgtype, short colType, int colnum, int txts);
#endif
void         checkDataType       (short db2type, int scale, Oid pgtype, const char* tablename, const char* colname);
List*        serializePlanData   (DB2FdwState* fdwState);
Const*       serializeString     (const char* s);
Const*       serializeLong       (long i);

/** db2PlanForeignModify
 *   Construct an DB2FdwState or copy it from the foreign scan plan.
 *   Construct the DB2 DML statement and a list of necessary parameters.
 *   Return the serialized DB2FdwState.
 */
List* db2PlanForeignModify (PlannerInfo* root, ModifyTable* plan, Index resultRelation, int subplan_index) {
  CmdType           operation     = plan->operation;
  RangeTblEntry*    rte           = planner_rt_fetch (resultRelation, root);
  Relation          rel           = NULL;
  StringInfoData    sql;
  List*             targetAttrs   = NIL;
  List*             returningList = NIL;
  DB2FdwState*      fdwState      = NULL;
  int               attnum        = 0;
  int               i             = 0;
  ListCell*         cell          = NULL;
  bool              has_trigger   = false, firstcol;
  ParamDesc*        param         = NULL;
  TupleDesc         tupdesc;
  Bitmapset*        updated_cols  = NULL;
  AttrNumber        col;
  int               col_idx       = -1;
  List*             result        = NIL;
  /*
   * Get the updated columns and the user for permission checks.
   * We put that here at the beginning, since the way to do that changed
   * considerably over the different PostgreSQL versions.
   */
#if PG_VERSION_NUM >= 160000
  RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);
  updated_cols = bms_copy(perminfo->updatedCols);
#else
#if PG_VERSION_NUM >= 90500
  updated_cols = bms_copy(rte->updatedCols);
#else
  updated_cols = bms_copy(rte->modifiedCols);
#endif  /* PG_VERSION_NUM >= 90500 */
#endif  /* PG_VERSION_NUM >= 160000 */
  db2Debug1("> db2PlanForeignModify");

#if PG_VERSION_NUM >= 90500
/* we don't support INSERT ... ON CONFLICT */
  if (plan->onConflictAction != ONCONFLICT_NONE)
    ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION), errmsg("INSERT with ON CONFLICT clause is not supported")));
#endif  /* PG_VERSION_NUM */

  /* check if the foreign table is scanned and we already planned that scan */
  if (resultRelation < root->simple_rel_array_size 
  &&  root->simple_rel_array[resultRelation]              != NULL 
  &&  root->simple_rel_array[resultRelation]->fdw_private != NULL) {
    /* if yes, copy the foreign table information from the associated RelOptInfo */
    fdwState = copyPlanData((DB2FdwState*)(root->simple_rel_array[resultRelation]->fdw_private));
  } else {
    /*
     * If no, we have to construct the foreign table data ourselves.
     * To match what ExecCheckRTEPerms does, pass the user whose user mapping
     * should be used (if invalid, the current user is used).
     */
    fdwState = db2GetFdwState(rte->relid, NULL);
  }
  initStringInfo(&sql);

  /*
   * Core code already has some lock on each rel being planned, so we can
   * use NoLock here.
   */
  rel = table_open(rte->relid, NoLock);

  /* figure out which attributes are affected and if there is a trigger */
  switch (operation) {
    case CMD_INSERT:
      /*
       * In an INSERT, we transmit all columns that are defined in the foreign
       * table.  In an UPDATE, we transmit only columns that were explicitly
       * targets of the UPDATE, so as to avoid unnecessary data transmission.
       * (We can't do that for INSERT since we would miss sending default values
       * for columns not listed in the source statement.)
       */
      tupdesc = RelationGetDescr (rel);
      for (attnum = 1; attnum <= tupdesc->natts; attnum++) {
        Form_pg_attribute attr = TupleDescAttr (tupdesc, attnum - 1);
        if (!attr->attisdropped)
          targetAttrs = lappend_int (targetAttrs, attnum);
      }
      /* is there a row level AFTER trigger? */
      has_trigger = rel->trigdesc && rel->trigdesc->trig_insert_after_row;
  
    break;
    case CMD_UPDATE:
      while ((col_idx = bms_next_member (updated_cols,col_idx)) >= 0) {
        col = col_idx + FirstLowInvalidHeapAttributeNumber;
        if (col <= InvalidAttrNumber)	/* shouldn't happen */
          elog (ERROR, "system-column update is not supported");
        targetAttrs = lappend_int (targetAttrs, col);
      }
      /* is there a row level AFTER trigger? */
      has_trigger = rel->trigdesc && rel->trigdesc->trig_update_after_row;
    break;
    case CMD_DELETE:
      /* is there a row level AFTER trigger? */
      has_trigger = rel->trigdesc && rel->trigdesc->trig_delete_after_row;
    break;
    default:
      elog (ERROR, "unexpected operation: %d", (int) operation);
    break;
  }
  table_close (rel, NoLock);
  /* mark all attributes for which we need a RETURNING clause */
  if (has_trigger) {
    /* all attributes are needed for the RETURNING clause */
    for (i = 0; i < fdwState->db2Table->ncols; ++i) {
      if (fdwState->db2Table->cols[i]->pgname != NULL) {
        /* throw an error if it is a LONG or LONG RAW column */
        short dbType = c2dbType(fdwState->db2Table->cols[i]->colType);
        if (dbType == DB2_BIGINT) {
          ereport ( ERROR
                  , ( errcode (ERRCODE_FDW_INVALID_DATA_TYPE)
                    , errmsg ("columns with DB2 type LONG or LONG RAW cannot be used in RETURNING clause")
                    , errdetail ("Column \"%s\" of foreign table \"%s\" is of DB2 type LONG%s."
                                , fdwState->db2Table->cols[i]->pgname
                                , fdwState->db2Table->pgname
                                , dbType == DB2_BIGINT ? "" : " RAW"
                                )
                    )
                  );
        }
        fdwState->db2Table->cols[i]->used = 1;
      }
    }
  } else {
    Bitmapset *attrs_used = NULL;
    /* extract the relevant RETURNING list if any */
    if (plan->returningLists)
      returningList = (List *) list_nth (plan->returningLists, subplan_index);
    if (returningList != NIL) {
      /* get all the attributes mentioned there */
      pull_varattnos ((Node *) returningList, resultRelation, &attrs_used);
      /* mark the corresponding columns as used */
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
        /* ignore columns that are not in the PostgreSQL table */
        if (fdwState->db2Table->cols[i]->pgname == NULL)
          continue;
        if (bms_is_member (fdwState->db2Table->cols[i]->pgattnum - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
          /* throw an error if it is a LONG or LONG RAW column */
          fdwState->db2Table->cols[i]->used = 1;
        }
      }
    }
  }
  /* construct the SQL command string */
  switch (operation) {
    case CMD_INSERT:
      appendStringInfo (&sql, "INSERT INTO %s (", fdwState->db2Table->name);
      firstcol = true;
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
        /* don't add columns beyond the end of the PostgreSQL table */
        if (fdwState->db2Table->cols[i]->pgname == NULL)
          continue;
        if (firstcol)
          firstcol = false;
        else
          appendStringInfo (&sql, ", ");
        appendStringInfo (&sql, "%s", fdwState->db2Table->cols[i]->colName);
      }
      appendStringInfo (&sql, ") VALUES (");
      firstcol = true;
      for (i = 0; i < fdwState->db2Table->ncols; ++i) {
        /* don't add columns beyond the end of the PostgreSQL table */
        if (fdwState->db2Table->cols[i]->pgname == NULL)
          continue;
        /* check that the data types can be converted */
        checkDataType (fdwState->db2Table->cols[i]->colType, fdwState->db2Table->cols[i]->colScale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);
        /* add a parameter description for the column */
        addParam (&fdwState->paramList, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->cols[i]->colType, i, 0);
        /* add parameter name */
        if (firstcol)
          firstcol = false;
        else
          appendStringInfo (&sql, ", ");
        appendAsType (&sql, fdwState->db2Table->cols[i]->pgtype);
      }
      appendStringInfo (&sql, ")");
    break;
    case CMD_UPDATE:
      appendStringInfo (&sql, "UPDATE %s SET ", fdwState->db2Table->name);
      firstcol = true;
      i = 0;
      foreach (cell, targetAttrs) {
        /* find the corresponding db2Table entry */
        while (i < fdwState->db2Table->ncols && fdwState->db2Table->cols[i]->pgattnum < lfirst_int (cell))
          ++i;
        if (i == fdwState->db2Table->ncols)
          break;
        /* ignore columns that don't occur in the foreign table */
        if (fdwState->db2Table->cols[i]->pgtype == 0)
          continue;
        /* check that the data types can be converted */
        checkDataType (fdwState->db2Table->cols[i]->colType, fdwState->db2Table->cols[i]->colScale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);
        /* add a parameter description for the column */
        addParam (&fdwState->paramList, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->cols[i]->colType, i, 0);
        /* add the parameter name to the query */
        if (firstcol)
          firstcol = false;
        else
          appendStringInfo (&sql, ", ");
        appendStringInfo (&sql, "%s = ", fdwState->db2Table->cols[i]->colName);
        appendAsType (&sql, fdwState->db2Table->cols[i]->pgtype);
      }
      /* throw a meaningful error if nothing is updated */
      if (firstcol)
        ereport (ERROR
                , (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION)
                           , errmsg ("no DB2 column modified by UPDATE")
                           , errdetail ("The UPDATE statement only changes colums that do not exist in the DB2 table.")
                  )
                );
    break;
    case CMD_DELETE:
      appendStringInfo (&sql, "DELETE FROM %s", fdwState->db2Table->name);
      break;
    default:
      elog (ERROR, "unexpected operation: %d", (int) operation);
    break;
  }
  if (operation == CMD_UPDATE || operation == CMD_DELETE) {
    /* add WHERE clause with the primary key columns */
    firstcol = true;
    for (i = 0; i < fdwState->db2Table->ncols; ++i) {
      if (fdwState->db2Table->cols[i]->pkey) {
        /* only set the flag here because we only retrieve the old key values in update or delete cases, later in setModifyParms*/
        fdwState->db2Table->cols[i]->colPrimKeyPart = 1;
        /* add a parameter description */
        addParam (&fdwState->paramList, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->cols[i]->colType, i, 0);
        /* add column and parameter name to query */
        if (firstcol) {
          appendStringInfo (&sql, " WHERE");
          firstcol = false;
        } else {
          appendStringInfo (&sql, " AND");
        }
        appendStringInfo (&sql, " %s = ", fdwState->db2Table->cols[i]->colName);
        appendAsType (&sql, fdwState->db2Table->cols[i]->pgtype);
      }
    }
  }
  /* add RETURNING clause if appropriate */
  firstcol = true;
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    if (fdwState->db2Table->cols[i]->used) {
      if (firstcol) {
        firstcol = false;
        appendStringInfo (&sql, " RETURNING ");
      } else {
        appendStringInfo (&sql, ", ");
      }
      appendStringInfo (&sql, "%s", fdwState->db2Table->cols[i]->colName);
    }
  }
    /* add the parameters for the RETURNING clause */
  firstcol = true;
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    if (fdwState->db2Table->cols[i]->used) {
      /* check that the data types can be converted */
      checkDataType (fdwState->db2Table->cols[i]->colType, fdwState->db2Table->cols[i]->colScale, fdwState->db2Table->cols[i]->pgtype, fdwState->db2Table->pgname, fdwState->db2Table->cols[i]->pgname);

      /* create a new entry in the parameter list */
      param = (ParamDesc *) palloc (sizeof (ParamDesc));
      param->type         = fdwState->db2Table->cols[i]->pgtype;
      param->bindType     = BIND_OUTPUT;
      param->value        = NULL;
      param->node         = NULL;
      param->colnum       = i;
      param->next         = fdwState->paramList;
      fdwState->paramList = param;

      if (firstcol) {
        firstcol = false;
        appendStringInfo (&sql, " INTO ");
      } else {
        appendStringInfo (&sql, ", ");
      }
      appendStringInfo (&sql, "?");
    }
  }
  fdwState->query = sql.data;
  db2Debug2("  fdwState->query: '%s'", fdwState->query);
  /* return a serialized form of the plan state */
  result = serializePlanData (fdwState);
  db2Debug1("> db2PlanForeignModify");
  return result;
}

#ifdef WRITE_API
/** copyPlanData
 *   Create a deep copy of the argument, copy only those fields needed for planning.
 */
DB2FdwState* copyPlanData (DB2FdwState* orig) {
  int          i    = 0;
  DB2FdwState* copy = NULL;

  db2Debug1("> copyPlanData");
  copy                    = db2Alloc(sizeof (DB2FdwState));
  copy->dbserver          = pstrdup (orig->dbserver);
  copy->user              = pstrdup (orig->user);
  copy->password          = pstrdup (orig->password);
  copy->nls_lang          = pstrdup (orig->nls_lang);
  copy->session           = NULL;
  copy->query             = NULL;
  copy->paramList         = NULL;
  copy->db2Table          = (DB2Table*) db2Alloc(sizeof (DB2Table));
  copy->db2Table->name    = pstrdup (orig->db2Table->name);
  copy->db2Table->pgname  = pstrdup (orig->db2Table->pgname);
  copy->db2Table->ncols   = orig->db2Table->ncols;
  copy->db2Table->npgcols = orig->db2Table->npgcols;
  copy->db2Table->cols    = (DB2Column**) db2Alloc(sizeof (DB2Column*) * orig->db2Table->ncols);
  for (i = 0; i < orig->db2Table->ncols; ++i) {
    copy->db2Table->cols[i]                 = (DB2Column*) db2Alloc(sizeof (DB2Column));
    copy->db2Table->cols[i]->colName        = pstrdup (orig->db2Table->cols[i]->colName);
    copy->db2Table->cols[i]->colType        = orig->db2Table->cols[i]->colType;
    copy->db2Table->cols[i]->colSize        = orig->db2Table->cols[i]->colSize;
    copy->db2Table->cols[i]->colScale       = orig->db2Table->cols[i]->colScale;
    copy->db2Table->cols[i]->colNulls       = orig->db2Table->cols[i]->colNulls;
    copy->db2Table->cols[i]->colChars       = orig->db2Table->cols[i]->colChars;
    copy->db2Table->cols[i]->colBytes       = orig->db2Table->cols[i]->colBytes;
    copy->db2Table->cols[i]->colPrimKeyPart = orig->db2Table->cols[i]->colPrimKeyPart;
    copy->db2Table->cols[i]->colCodepage    = orig->db2Table->cols[i]->colCodepage;
    if (orig->db2Table->cols[i]->pgname == NULL)
      copy->db2Table->cols[i]->pgname       = NULL;
    else
      copy->db2Table->cols[i]->pgname       = pstrdup (orig->db2Table->cols[i]->pgname);
    copy->db2Table->cols[i]->pgattnum       = orig->db2Table->cols[i]->pgattnum;
    copy->db2Table->cols[i]->pgtype         = orig->db2Table->cols[i]->pgtype;
    copy->db2Table->cols[i]->pgtypmod       = orig->db2Table->cols[i]->pgtypmod;
    copy->db2Table->cols[i]->used           = 0;
    copy->db2Table->cols[i]->pkey           = orig->db2Table->cols[i]->pkey;
    copy->db2Table->cols[i]->val            = NULL;
    copy->db2Table->cols[i]->val_size       = orig->db2Table->cols[i]->val_size;
    copy->db2Table->cols[i]->val_len        = 0;
    copy->db2Table->cols[i]->val_null       = 0;
  }
  copy->startup_cost = 0.0;
  copy->total_cost   = 0.0;
  copy->rowcount     = 0;
  copy->columnindex  = 0;
  copy->temp_cxt     = NULL;
  copy->order_clause = NULL;
  db2Debug1("< copyPlanData");
  return copy;
}

/** addParam
 *   Creates a new ParamDesc with the given values and adds it to the list.
 *   A palloc'ed copy of "name" is used.
 */
void addParam (ParamDesc **paramList, Oid pgtype, short colType, int colnum, int txts) {
  ParamDesc *param;

  db2Debug1(">  addParam");
  param       = palloc (sizeof (ParamDesc));
  param->type = pgtype;
  switch (c2dbType(colType)) {
    case DB2_INTEGER:
    case DB2_NUMERIC:
    case DB2_BIGINT:
    case DB2_SMALLINT:
    case DB2_FLOAT:
    case DB2_REAL:
    case DB2_DOUBLE:
      param->bindType = BIND_NUMBER;
    break;
    case DB2_CLOB:
      param->bindType = BIND_LONG;
    break;
    case DB2_BLOB:
      param->bindType = BIND_LONGRAW;
    break;
    default:
      param->bindType = BIND_STRING;
  }
  param->value  = NULL;
  param->node   = NULL;
  param->colnum = colnum;
  param->txts   = txts;
  db2Debug2("  param->colnum: '%d'",param->colnum);
  param->next   = *paramList;
  *paramList    = param;
  db2Debug1(">  addParam");
}
#endif /* WRITE_API */

/** checkDataType
 *   Check that the DB2 data type of a column can be
 *   converted to the PostgreSQL data type, raise an error if not.
 */
void checkDataType (short sqltype, int scale, Oid pgtype, const char *tablename, const char *colname) {
  short db2type = c2dbType(sqltype);
  db2Debug1("> checkDataType");
  db2Debug2("  checkDataType: %s.$s of sqltype: %d, db2type: %d, pgtype: %d",tablename,colname,sqltype, db2type, pgtype);
  /* the binary DB2 types can be converted to bytea */
  if (db2type == DB2_BLOB && pgtype == BYTEAOID) {
    db2Debug3("  DB2_BLOB can be converted into BYTEAOID");
  } else if (db2type == DB2_XML && pgtype == XMLOID) {
    db2Debug3("  DB2_XML can be converted into XMLOID");
  } else if (db2type != DB2_UNKNOWN_TYPE && db2type != DB2_BLOB && (pgtype == TEXTOID || pgtype == VARCHAROID || pgtype == BPCHAROID)) {
    db2Debug3("  DB2_UNKNONW && not DB2_BLOB can be converted into TEXTOID, VARCHAROID, BPCHAROID");
  } else if ((db2type == DB2_INTEGER || db2type == DB2_SMALLINT || db2type == DB2_BIGINT || db2type == DB2_FLOAT || db2type == DB2_DOUBLE || db2type == DB2_REAL || db2type == DB2_DECIMAL || db2type == DB2_DECFLOAT) && (pgtype == NUMERICOID || pgtype == FLOAT4OID || pgtype == FLOAT8OID)) {
    db2Debug3("  DB2_INTEGER,SMALLINT,BIGINT,FLOAT,DOUBLE,REAL,DECIMAL,DECFLOAT can be converted into NUMERICOID,FLOAT4OID,FLOAT8OID");
  } else if ((db2type == DB2_INTEGER || db2type == DB2_SMALLINT || db2type == DB2_BIGINT || db2type == DB2_BOOLEAN) && scale <= 0 && (pgtype == INT2OID || pgtype == INT4OID || pgtype == INT8OID || pgtype == BOOLOID)) {
    db2Debug3("  DB2_INTEGER,SMALLINT,BIGINT,BOOLEAN can be converted into INT2OID, INT42OID, INT8OID,BOOLOID");
  } else if ((db2type == DB2_TYPE_DATE || db2type == DB2_TYPE_TIME || db2type == DB2_TYPE_TIMESTAMP || db2type == DB2_TYPE_TIMESTAMP_WITH_TIMEZONE) && (pgtype == DATEOID || pgtype == TIMESTAMPOID || pgtype == TIMESTAMPTZOID || pgtype == TIMEOID || pgtype == TIMETZOID)) {
    db2Debug3("  DB2_TYPE_DATE,TIME,TIMESTAMP,TIMESTAMP_WITH_TIMEZONE can be converted into DATEOID,TIMESTAMPOID,TIMESTAMPTZOID,TIMEOID,TIMETZOID");
  } else if ((db2type == DB2_VARCHAR || db2type == DB2_CLOB) && pgtype == JSONOID) {
    db2Debug3("  DB2_VARCHAR or DB2_CLOB can be converted into JSONOID");
  } else {
    /* nok - report an error */
    ereport ( ERROR
            , ( errcode (ERRCODE_FDW_INVALID_DATA_TYPE)
              , errmsg ( "column \"%s\" of type \"%d\" of foreign DB2 table \"%s\" cannot be converted to \"%d\" "
                       , colname
                       , db2type
                       , tablename
                       , pgtype)
              )
            );
  }
  db2Debug1("< checkDataType");
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
  result = lappend (result, serializeInt ((int) fdwState->prefetch));
  /* DB2 table name */
  result = lappend (result, serializeString (fdwState->db2Table->name));
  /* PostgreSQL table name */
  result = lappend (result, serializeString (fdwState->db2Table->pgname));
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
Const* serializeString (const char* s) {
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
Const* serializeLong (long i) {
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
