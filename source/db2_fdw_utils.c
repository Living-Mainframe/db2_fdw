#include <postgres.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_proc.h>
#include <commands/vacuum.h>
#include <mb/pg_wchar.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include <utils/date.h>
#include <utils/datetime.h>
#include <utils/guc.h>
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

/** external prototypes */
extern void            db2GetLob                 (DB2Session* session, DB2Column* column, int cidx, char** value, long* value_len, unsigned long trunc);
extern void            db2Shutdown               (void);
extern short           c2dbType                  (short fcType);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2Debug2                 (const char* message, ...);
extern void            db2Debug3                 (const char* message, ...);

/** local prototypes */
void                appendAsType              (StringInfoData* dest, Oid type);
char*               deparseExpr               (DB2Session* session, RelOptInfo * foreignrel, Expr* expr, const DB2Table* db2Table, List** params);
char*               datumToString             (Datum datum, Oid type);
char*               guessNlsLang              (char* nls_lang);
char*               deparseDate               (Datum datum);
char*               deparseTimestamp          (Datum datum, bool hasTimezone);
char*               deparseInterval           (Datum datum);
void                exitHook                  (int code, Datum arg);
void                convertTuple              (DB2FdwState* fdw_state, Datum* values, bool* nulls, bool trunc_lob) ;
void                errorContextCallback      (void* arg);

/** appendAsType
 *   Append "s" to "dest", adding appropriate casts for datetime "type".
 */
void appendAsType (StringInfoData* dest, Oid type) {
  db2Debug1("> %s::appendAsType", __FILE__);
  db2Debug2("  dest->data: '%s'",dest->data);
  db2Debug2("  type: %d",type);
  switch (type) {
    case DATEOID:
      appendStringInfo (dest, "CAST (? AS DATE)");
      break;
    case TIMESTAMPOID:
      appendStringInfo (dest, "CAST (? AS TIMESTAMP)");
      break;
    case TIMESTAMPTZOID:
      appendStringInfo (dest, "CAST (? AS TIMESTAMP)");
      break;
    case TIMEOID:
      appendStringInfo (dest, "(CAST (? AS TIME))");
      break;
    case TIMETZOID:
      appendStringInfo (dest, "(CAST (? AS TIME))");
      break;
    default:
      appendStringInfo (dest, "?");
    break;
  }
  db2Debug2("  dest->data: '%s'", dest->data);
  db2Debug1("< %s::appendAsType", __FILE__);
}

/** This macro is used by deparseExpr to identify PostgreSQL
 * types that can be translated to DB2 SQL.
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
      || (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
      || (x) == INT4OID || (x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID \
      || (x) == NUMERICOID || (x) == DATEOID || (x) == TIMEOID || (x) == TIMESTAMPOID \
      || (x) == TIMESTAMPTZOID || (x) == INTERVALOID)

/** deparseExpr
 *   Create and return an DB2 SQL string from "expr".
 *   Returns NULL if that is not possible, else a palloc'ed string.
 *   As a side effect, all Params incorporated in the WHERE clause
 *   will be stored in "params".
 */
char* deparseExpr (DB2Session* session, RelOptInfo* foreignrel, Expr* expr, const DB2Table* db2Table, List** params) {
  char*              opername;
  char*              left;
  char*              right;
  char*              arg;
  char               oprkind;
  #ifndef OLD_FDW_API
  char               parname[10];
  #endif
  Const*             constant;
  OpExpr*            oper;
  ScalarArrayOpExpr* arrayoper;
  CaseExpr*          caseexpr;
  BoolExpr*          boolexpr;
  CoalesceExpr*      coalesceexpr;
  CoerceViaIO*       coerce;
  Param*             param;
  Var*               variable;
  FuncExpr*          func;
  Expr*              rightexpr;
  ArrayExpr*         array;
  ArrayCoerceExpr*   arraycoerce;
  #if PG_VERSION_NUM >= 100000
  SQLValueFunction*  sqlvalfunc;
  #endif
  regproc            typoutput;
  HeapTuple          tuple;
  ListCell*          cell;
  StringInfoData     result;
  Oid                leftargtype;
  Oid                rightargtype;
  Oid                schema;
  short              db2type;
  ArrayIterator      iterator;
  Datum              datum;
  bool               first_arg;
  bool               isNull;
  int                index;
  StringInfoData     alias;
  const DB2Table*    var_table;  /* db2Table that belongs to a Var */
  db2Debug1("> %s::deparseExpr", __FILE__);
  db2Debug2("  expr: %x",expr);
  if (expr != NULL) {
    switch (expr->type) {
      case T_Const: {
        constant = (Const *) expr;
        if (constant->constisnull) {
          /* only translate NULLs of a type DB2 can handle */
          if (canHandleType (constant->consttype)) {
            initStringInfo (&result);
            appendStringInfo (&result, "NULL");
          } else {
            return NULL;
          }
        } else {
          /* get a string representation of the value */
          char *c = datumToString (constant->constvalue, constant->consttype);
          if (c == NULL) {
            return NULL;
          } else {
            initStringInfo (&result);
            appendStringInfo (&result, "%s", c);
          }
        }
      }
      break;
      case T_Param: {
        param = (Param *) expr;
        #ifdef OLD_FDW_API
        /* don't try to push down parameters with 9.1 */
        return NULL;
        #else
        /* don't try to handle interval parameters */
        if (!canHandleType (param->paramtype) || param->paramtype == INTERVALOID)
          return NULL;
    
        /* find the index in the parameter list */
        index = 0;
        foreach (cell, *params) {
          ++index;
          if (equal (param, (Node *) lfirst (cell)))
            break;
        }
        if (cell == NULL) {
          /* add the parameter to the list */
          ++index;
          *params = lappend (*params, param);
        }
    
        /* parameters will be called :p1, :p2 etc. */
        snprintf (parname, 10, ":p%d", index);
        initStringInfo (&result);
        appendAsType (&result, param->paramtype);
        #endif /* OLD_FDW_API */
      }
      break;
      case T_Var: {
        variable = (Var *) expr;
        var_table = NULL;
    
        /* check if the variable belongs to one of our foreign tables */
        #ifdef JOIN_API
        if (IS_SIMPLE_REL (foreignrel)) {
        #endif /* JOIN_API */
          if (variable->varno == foreignrel->relid && variable->varlevelsup == 0)
            var_table = db2Table;
        #ifdef JOIN_API
        } else {
          DB2FdwState* joinstate  = (DB2FdwState*) foreignrel->fdw_private;
          DB2FdwState* outerstate = (DB2FdwState*) joinstate->outerrel->fdw_private;
          DB2FdwState* innerstate = (DB2FdwState*) joinstate->innerrel->fdw_private;
    
          /* we can't get here if the foreign table has no columns, so this is safe */
          if (variable->varno == outerstate->db2Table->cols[0]->varno && variable->varlevelsup == 0)
            var_table = outerstate->db2Table;
          if (variable->varno == innerstate->db2Table->cols[0]->varno && variable->varlevelsup == 0)
            var_table = innerstate->db2Table;
        }
        #endif /* JOIN_API */
        if (var_table) {
          /* the variable belongs to a foreign table, replace it with the name */
          /* we cannot handle system columns */
          if (variable->varattno < 1)
            return NULL;
          /*
           * Allow boolean columns here.
           * They will be rendered as ("COL" <> 0).
           */
          if (!(canHandleType (variable->vartype) || variable->vartype == BOOLOID))
            return NULL;
          /* get var_table column index corresponding to this column (-1 if none) */
          index = var_table->ncols - 1;
          while (index >= 0 && var_table->cols[index]->pgattnum != variable->varattno)
            --index;
          /* if no DB2 column corresponds, translate as NULL */
          if (index == -1) {
            initStringInfo (&result);
            appendStringInfo (&result, "NULL");
            break;
          }
    
          /*
           * Don't try to convert a column reference if the type is
           * converted from a non-string type in DB2 to a string type
           * in PostgreSQL because functions and operators won't work the same.
           */
          db2type = c2dbType(var_table->cols[index]->colType);
          if ((variable->vartype == TEXTOID || variable->vartype == BPCHAROID || variable->vartype == VARCHAROID)  && db2type != DB2_VARCHAR && db2type != DB2_CHAR)
            return NULL;
          initStringInfo (&result);
          /* work around the lack of booleans in DB2 */
          if (variable->vartype == BOOLOID) {
            appendStringInfo (&result, "(");
          }
    
          /* qualify with an alias based on the range table index */
          initStringInfo (&alias);
          ADD_REL_QUALIFIER (&alias, var_table->cols[index]->varno);
    
          appendStringInfo (&result, "%s%s", alias.data, var_table->cols[index]->colName);
    
          /* work around the lack of booleans in DB2 */
          if (variable->vartype == BOOLOID) {
            appendStringInfo (&result, " <> 0)");
          }
        } else {
          /* treat it like a parameter */
          #ifdef OLD_FDW_API
          /* don't try to push down parameters with 9.1 */
          return NULL;
          #else
          /* don't try to handle type interval */
          if (!canHandleType (variable->vartype) || variable->vartype == INTERVALOID)
            return NULL;
    
          /* find the index in the parameter list */
          index = 0;
          foreach (cell, *params) {
            ++index;
            if (equal (variable, (Node *) lfirst (cell)))
              break;
          }
          if (cell == NULL) {
            /* add the parameter to the list */
            ++index;
            *params = lappend (*params, variable);
          }
    
          /* parameters will be called :p1, :p2 etc. */
          initStringInfo (&result);
          appendStringInfo (&result, ":p%d", index);
          #endif /* OLD_FDW_API */
        }
      }
      break;
      case T_OpExpr: {
        oper = (OpExpr *) expr;
        /* get operator name, kind, argument type and schema */
        tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (oper->opno));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for operator %u", oper->opno);
        }
        opername = pstrdup (((Form_pg_operator) GETSTRUCT (tuple))->oprname.data);
        oprkind = ((Form_pg_operator) GETSTRUCT (tuple))->oprkind;
        leftargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
        rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
        schema = ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
        ReleaseSysCache (tuple);
        /* ignore operators in other than the pg_catalog schema */
        if (schema != PG_CATALOG_NAMESPACE)
          return NULL;
        if (!canHandleType (rightargtype))
          return NULL;
        /*
         * Don't translate operations on two intervals.
         * INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND don't mix well.
         */
        if (leftargtype == INTERVALOID && rightargtype == INTERVALOID)
          return NULL;
        /* the operators that we can translate */
        if (strcmp (opername, "=") == 0 || strcmp (opername, "<>") == 0
        /* string comparisons are not safe */
        || (strcmp (opername, ">")  == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID && rightargtype != NAMEOID && rightargtype != CHAROID)
        || (strcmp (opername, "<")  == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID && rightargtype != NAMEOID && rightargtype != CHAROID)
        || (strcmp (opername, ">=") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID && rightargtype != NAMEOID && rightargtype != CHAROID)
        || (strcmp (opername, "<=") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID && rightargtype != NAMEOID && rightargtype != CHAROID)
        || strcmp (opername, "+")   == 0
        /* subtracting DATEs yields a NUMBER in DB2 */
        || (strcmp (opername, "-")    == 0 && rightargtype != DATEOID      && rightargtype != TIMESTAMPOID  && rightargtype != TIMESTAMPTZOID)
        ||  strcmp (opername, "*")    == 0 || strcmp (opername, "~~") == 0 || strcmp (opername, "!~~") == 0 || strcmp (opername, "~~*") == 0
        ||  strcmp (opername, "!~~*") == 0 || strcmp (opername, "^")  == 0 || strcmp (opername, "%")   == 0 || strcmp (opername, "&")   == 0 
        ||  strcmp (opername, "|/")   == 0 || strcmp (opername, "@")  == 0) {
          left = deparseExpr (session, foreignrel, linitial (oper->args), db2Table, params);
          if (left == NULL) {
            pfree (opername);
            return NULL;
          }
    
          if (oprkind == 'b') {
            /* binary operator */
            right = deparseExpr (session, foreignrel, lsecond (oper->args), db2Table, params);
            if (right == NULL) {
              pfree (left);
              pfree (opername);
              return NULL;
            }
            initStringInfo (&result);
            if (strcmp (opername, "~~") == 0) {
              appendStringInfo (&result, "(%s LIKE %s ESCAPE '\\')", left, right);
            } else if (strcmp (opername, "!~~") == 0) {
              appendStringInfo (&result, "(%s NOT LIKE %s ESCAPE '\\')", left, right);
            } else if (strcmp (opername, "~~*") == 0) {
              appendStringInfo (&result, "(UPPER(%s) LIKE UPPER(%s) ESCAPE '\\')", left, right);
            } else if (strcmp (opername, "!~~*") == 0) {
              appendStringInfo (&result, "(UPPER(%s) NOT LIKE UPPER(%s) ESCAPE '\\')", left, right);
            } else if (strcmp (opername, "^") == 0) {
              appendStringInfo (&result, "POWER(%s, %s)", left, right);
            } else if (strcmp (opername, "%") == 0) {
              appendStringInfo (&result, "MOD(%s, %s)", left, right);
            } else if (strcmp (opername, "&") == 0) {
              appendStringInfo (&result, "BITAND(%s, %s)", left, right);
            } else {
              /* the other operators have the same name in DB2 */
              appendStringInfo (&result, "(%s %s %s)", left, opername, right);
            }
            pfree (right);
            pfree (left);
          } else {
            /* unary operator */
            initStringInfo (&result);
            if (strcmp (opername, "|/") == 0) {
              appendStringInfo (&result, "SQRT(%s)", left);
            } else if (strcmp (opername, "@") == 0) {
              appendStringInfo (&result, "ABS(%s)", left);
            } else {
              /* unary + or - */
              appendStringInfo (&result, "(%s%s)", opername, left);
            }
            pfree (left);
          }
        } else {
          /* cannot translate this operator */
          pfree (opername);
          return NULL;
        }
        pfree (opername);
      }
      break;
      case T_ScalarArrayOpExpr: {
        arrayoper = (ScalarArrayOpExpr *) expr;
    
        /* get operator name, left argument type and schema */
        tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (arrayoper->opno));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for operator %u", arrayoper->opno);
        }
        opername    = pstrdup (((Form_pg_operator) GETSTRUCT (tuple))->oprname.data);
        leftargtype =          ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
        schema      =          ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
        ReleaseSysCache (tuple);
    
        /* get the type's output function */
        tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (leftargtype));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for type %u", leftargtype);
        }
        typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
        ReleaseSysCache (tuple);
    
        /* ignore operators in other than the pg_catalog schema */
        if (schema != PG_CATALOG_NAMESPACE)
          return NULL;
    
        /* don't try to push down anything but IN and NOT IN expressions */
        if ((strcmp (opername, "=") != 0 || !arrayoper->useOr) && (strcmp (opername, "<>") != 0 || arrayoper->useOr))
          return NULL;
    
        if (!canHandleType (leftargtype))
          return NULL;
    
        left = deparseExpr (session, foreignrel, linitial (arrayoper->args), db2Table, params);
        if (left == NULL)
          return NULL;
    
        /* begin to compose result */
        initStringInfo (&result);
        appendStringInfo (&result, "(%s %s (", left, arrayoper->useOr ? "IN" : "NOT IN");
    
        /* the second (=last) argument can be Const, ArrayExpr or ArrayCoerceExpr */
        rightexpr = (Expr *) llast (arrayoper->args);
        switch (rightexpr->type) {
          case T_Const: {
            /* the second (=last) argument is a Const of ArrayType */
            constant = (Const *) rightexpr;
            /* using NULL in place of an array or value list is valid in DB2 and PostgreSQL */
            if (constant->constisnull) {
              appendStringInfo (&result, "NULL");
            } else {
              /* loop through the array elements */
              iterator = array_create_iterator (DatumGetArrayTypeP (constant->constvalue), 0);
              first_arg = true;
              while (array_iterate (iterator, &datum, &isNull)) {
                char *c;
                if (isNull) {
                  c = "NULL";
                } else {
                  c = datumToString (datum, leftargtype);
                  if (c == NULL) {
                    array_free_iterator (iterator);
                    return NULL;
                  }
                }
                /* append the argument */
                appendStringInfo (&result, "%s%s", first_arg ? "" : ", ", c);
                first_arg = false;
              }
              array_free_iterator (iterator);
              /* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
              if (first_arg)
                return NULL;
            }
          }
          break;
          case T_ArrayCoerceExpr: {
            /* the second (=last) argument is an ArrayCoerceExpr */
            arraycoerce = (ArrayCoerceExpr *) rightexpr;

            /* if the conversion requires more than binary coercion, don't push it down */
            #if PG_VERSION_NUM < 110000
            if (arraycoerce->elemfuncid != InvalidOid)
              return NULL;
            #else
            if (arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType)
              return NULL;
            #endif

            /* the actual array is here */
            rightexpr = arraycoerce->arg;
          }
          /* fall through ! */
          case T_ArrayExpr: {
            /* the second (=last) argument is an ArrayExpr */
            array = (ArrayExpr *) rightexpr;

            /* loop the array arguments */
            first_arg = true;
            foreach (cell, array->elements) {
              /* convert the argument to a string */
              char* element = deparseExpr (session, foreignrel, (Expr *) lfirst (cell), db2Table, params);

              /* if any element cannot be converted, give up */
              if (element == NULL)
                return NULL;

                /* append the argument */
              appendStringInfo (&result, "%s%s", first_arg ? "" : ", ", element);
              first_arg = false;
            }

            /* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
            if (first_arg)
              return NULL;
          }
          break;
          default:
            return NULL;
        }
        /* two parentheses close the expression */
        appendStringInfo (&result, "))");
      }
      break;
      case T_DistinctExpr: {
        /* get argument type */
        tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (((DistinctExpr *) expr)->opno));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for operator %u", ((DistinctExpr *) expr)->opno);
        }
        rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
        ReleaseSysCache (tuple);

        if (!canHandleType (rightargtype))
          return NULL;

        left = deparseExpr (session, foreignrel, linitial (((DistinctExpr *) expr)->args), db2Table, params);
        if (left == NULL) {
          return NULL;
        }
        right = deparseExpr (session, foreignrel, lsecond (((DistinctExpr *) expr)->args), db2Table, params);
        if (right == NULL) {
          pfree (left);
          return NULL;
        }

        initStringInfo (&result);
        appendStringInfo (&result, "(%s IS DISTINCT FROM %s)", left, right);
      }
      break;
      case T_NullIfExpr: {
        /* get argument type */
        tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (((NullIfExpr *) expr)->opno));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for operator %u", ((NullIfExpr *) expr)->opno);
        }
        rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
        ReleaseSysCache (tuple);

        if (!canHandleType (rightargtype))
          return NULL;

        left = deparseExpr (session, foreignrel, linitial (((NullIfExpr *) expr)->args), db2Table, params);
        if (left == NULL) {
          return NULL;
        }
        right = deparseExpr (session, foreignrel, lsecond (((NullIfExpr *) expr)->args), db2Table, params);
        if (right == NULL) {
          pfree (left);
          return NULL;
        }

        initStringInfo (&result);
        appendStringInfo (&result, "NULLIF(%s, %s)", left, right);
      }
      break;
      case T_BoolExpr: {
        boolexpr = (BoolExpr *) expr;

        arg = deparseExpr (session, foreignrel, linitial (boolexpr->args), db2Table, params);
        if (arg == NULL)
          return NULL;

        initStringInfo (&result);
        appendStringInfo (&result, "(%s%s", boolexpr->boolop == NOT_EXPR ? "NOT " : "", arg);

        do_each_cell(cell, boolexpr->args, list_next(boolexpr->args, list_head(boolexpr->args))) { 
          arg = deparseExpr (session, foreignrel, (Expr *) lfirst (cell), db2Table, params);
          if (arg == NULL) {
            pfree (result.data);
            return NULL;
          }
          appendStringInfo (&result, " %s %s", boolexpr->boolop == AND_EXPR ? "AND" : "OR", arg);
        }
        appendStringInfo (&result, ")");
      }
      break;
      case T_RelabelType: {
        return deparseExpr (session, foreignrel, ((RelabelType *) expr)->arg, db2Table, params);
      }
      break;
      case T_CoerceToDomain: {
        return deparseExpr (session, foreignrel, ((CoerceToDomain *) expr)->arg, db2Table, params);
      }
      break;
      case T_CaseExpr: {
        caseexpr = (CaseExpr *) expr;
        if (!canHandleType (caseexpr->casetype)) {
          return NULL;
        }
        initStringInfo (&result);
        appendStringInfo (&result, "CASE");
        /* for the form "CASE arg WHEN ...", add first expression */
        if (caseexpr->arg != NULL) {
          arg = deparseExpr (session, foreignrel, caseexpr->arg, db2Table, params);
          if (arg == NULL) {
            pfree (result.data);
            return NULL;
          } else {
            appendStringInfo (&result, " %s", arg);
          }
        }
        /* append WHEN ... THEN clauses */
        foreach (cell, caseexpr->args) {
          CaseWhen *whenclause = (CaseWhen *) lfirst (cell);
          /* WHEN */
          if (caseexpr->arg == NULL) {
            /* for CASE WHEN ..., use the whole expression */
            arg = deparseExpr (session, foreignrel, whenclause->expr, db2Table, params);
          } else {
            /* for CASE arg WHEN ..., use only the right branch of the equality */
            arg = deparseExpr (session, foreignrel, lsecond (((OpExpr *) whenclause->expr)->args), db2Table, params);
          }
          if (arg == NULL) {
            pfree (result.data);
            return NULL;
          } else {
            appendStringInfo (&result, " WHEN %s", arg);
            pfree (arg);
          }

          /* THEN */
          arg = deparseExpr (session, foreignrel, whenclause->result, db2Table, params);
          if (arg == NULL) {
            pfree (result.data);
            return NULL;
          } else {
            appendStringInfo (&result, " THEN %s", arg);
            pfree (arg);
          }
        }
        /* append ELSE clause if appropriate */
        if (caseexpr->defresult != NULL) {
          arg = deparseExpr (session, foreignrel, caseexpr->defresult, db2Table, params);
          if (arg == NULL) {
            pfree (result.data);
            return NULL;
          } else {
            appendStringInfo (&result, " ELSE %s", arg);
            pfree (arg);
          }
        }
        /* append END */
        appendStringInfo (&result, " END");
      }
      break;
      case T_CoalesceExpr: {
        coalesceexpr = (CoalesceExpr *) expr;

        if (!canHandleType (coalesceexpr->coalescetype))
          return NULL;

        initStringInfo (&result);
        appendStringInfo (&result, "COALESCE(");

        first_arg = true;
        foreach (cell, coalesceexpr->args) {
          arg = deparseExpr (session, foreignrel, (Expr *) lfirst (cell), db2Table, params);
          if (arg == NULL) {
            pfree (result.data);
            return NULL;
          }

          if (first_arg) {
            appendStringInfo (&result, "%s", arg);
            first_arg = false;
          } else {
            appendStringInfo (&result, ", %s", arg);
          }
          pfree (arg);
        }

        appendStringInfo (&result, ")");
      }
      break;
      case T_NullTest: {
        arg = deparseExpr (session, foreignrel, ((NullTest *) expr)->arg, db2Table, params);
        if (arg == NULL)
          return NULL;
    
        initStringInfo (&result);
        appendStringInfo (&result, "(%s IS %sNULL)", arg, ((NullTest *) expr)->nulltesttype == IS_NOT_NULL ? "NOT " : "");
      }
      break;
      case T_FuncExpr: {
        func = (FuncExpr *) expr;
    
        if (!canHandleType (func->funcresulttype))
          return NULL;
    
        /* do nothing for implicit casts */
        if (func->funcformat == COERCE_IMPLICIT_CAST)
          return deparseExpr (session, foreignrel, linitial (func->args), db2Table, params);
    
        /* get function name and schema */
        tuple = SearchSysCache1 (PROCOID, ObjectIdGetDatum (func->funcid));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for function %u", func->funcid);
        }
        opername = pstrdup (((Form_pg_proc) GETSTRUCT (tuple))->proname.data);
        schema = ((Form_pg_proc) GETSTRUCT (tuple))->pronamespace;
        ReleaseSysCache (tuple);
    
        /* ignore functions in other than the pg_catalog schema */
        if (schema != PG_CATALOG_NAMESPACE)
          return NULL;
    
        /* the "normal" functions that we can translate */
        if (strcmp (opername, "abs")         == 0 || strcmp (opername, "acos")         == 0 || strcmp (opername, "asin")             == 0
        || strcmp (opername, "atan")         == 0 || strcmp (opername, "atan2")        == 0 || strcmp (opername, "ceil")             == 0
        || strcmp (opername, "ceiling")      == 0 || strcmp (opername, "char_length")  == 0 || strcmp (opername, "character_length") == 0
        || strcmp (opername, "concat")       == 0 || strcmp (opername, "cos")          == 0 || strcmp (opername, "exp")              == 0
        || strcmp (opername, "initcap")      == 0 || strcmp (opername, "length")       == 0 || strcmp (opername, "lower")            == 0
        || strcmp (opername, "lpad")         == 0 || strcmp (opername, "ltrim")        == 0 || strcmp (opername, "mod")              == 0
        || strcmp (opername, "octet_length") == 0 || strcmp (opername, "position")     == 0 || strcmp (opername, "pow")              == 0
        || strcmp (opername, "power")        == 0 || strcmp (opername, "replace")      == 0 || strcmp (opername, "round")            == 0
        || strcmp (opername, "rpad")         == 0 || strcmp (opername, "rtrim")        == 0 || strcmp (opername, "sign")             == 0
        || strcmp (opername, "sin")          == 0 || strcmp (opername, "sqrt")         == 0 || strcmp (opername, "strpos")           == 0
        || strcmp (opername, "substr")       == 0 || strcmp (opername, "tan")          == 0 || strcmp (opername, "to_char")          == 0
        || strcmp (opername, "to_date")      == 0 || strcmp (opername, "to_number")    == 0 || strcmp (opername, "to_timestamp")     == 0
        || strcmp (opername, "translate")    == 0 || strcmp (opername, "trunc")        == 0 || strcmp (opername, "upper")            == 0
        ||(strcmp (opername, "substring")    == 0 && list_length (func->args) == 3)) {
          initStringInfo (&result);
    
          if (strcmp (opername, "ceiling") == 0)
            appendStringInfo (&result, "CEIL(");
          else if (strcmp (opername, "char_length") == 0 || strcmp (opername, "character_length") == 0)
            appendStringInfo (&result, "LENGTH(");
          else if (strcmp (opername, "pow") == 0)
            appendStringInfo (&result, "POWER(");
          else if (strcmp (opername, "octet_length") == 0)
            appendStringInfo (&result, "LENGTHB(");
          else if (strcmp (opername, "position") == 0 || strcmp (opername, "strpos") == 0)
            appendStringInfo (&result, "INSTR(");
          else if (strcmp (opername, "substring") == 0)
            appendStringInfo (&result, "SUBSTR(");
          else
            appendStringInfo (&result, "%s(", opername);
    
          first_arg = true;
          foreach (cell, func->args) {
            arg = deparseExpr (session, foreignrel, lfirst (cell), db2Table, params);
            if (arg == NULL) {
              pfree (result.data);
              pfree (opername);
              return NULL;
            }
            if (first_arg) {
              first_arg = false;
              appendStringInfo (&result, "%s", arg);
            } else {
              appendStringInfo (&result, ", %s", arg);
            }
            pfree (arg);
          }
          appendStringInfo (&result, ")");
        } else if (strcmp (opername, "date_part") == 0) {
          /* special case: EXTRACT */
          left = deparseExpr (session, foreignrel, linitial (func->args), db2Table, params);
          if (left == NULL) {
            pfree (opername);
            return NULL;
          }
          /* can only handle these fields in DB2 */
          if (strcmp (left, "'year'")          == 0 || strcmp (left, "'month'")           == 0
          ||  strcmp (left, "'day'")           == 0 || strcmp (left, "'hour'")            == 0
          || strcmp (left, "'minute'")         == 0 || strcmp (left, "'second'")          == 0
          ||  strcmp (left, "'timezone_hour'") == 0 || strcmp (left, "'timezone_minute'") == 0) {
            /* remove final quote */
            left[strlen (left) - 1] = '\0';
    
            right = deparseExpr (session, foreignrel, lsecond (func->args), db2Table, params);
            if (right == NULL) {
              pfree (opername);
              pfree (left);
              return NULL;
            }
    
            initStringInfo (&result);
            appendStringInfo (&result, "EXTRACT(%s FROM %s)", left + 1, right);
          } else {
            pfree (opername);
            pfree (left);
            return NULL;
          }
    
          pfree (left);
          pfree (right);
        } else if (strcmp (opername, "now") == 0 || strcmp (opername, "transaction_timestamp") == 0) {
          /* special case: current timestamp */
          initStringInfo (&result);
          appendStringInfo (&result, "(CAST (?/*:now*/ AS TIMESTAMP))");
        } else {
          /* function that we cannot render for DB2 */
          pfree (opername);
          return NULL;
        }
        pfree (opername);
      }
      break;
      case T_CoerceViaIO: {
        /*
         * We will only handle casts of 'now'.
         */
        coerce = (CoerceViaIO *) expr;
    
        /* only casts to these types are handled */
        if (coerce->resulttype != DATEOID && coerce->resulttype != TIMESTAMPOID && coerce->resulttype != TIMESTAMPTZOID)
          return NULL;
    
        /* the argument must be a Const */
        if (coerce->arg->type != T_Const)
          return NULL;
    
        /* the argument must be a not-NULL text constant */
        constant = (Const *) coerce->arg;
        if (constant->constisnull || (constant->consttype != CSTRINGOID && constant->consttype != TEXTOID))
          return NULL;
    
        /* get the type's output function */
        tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (constant->consttype));
        if (!HeapTupleIsValid (tuple)) {
          elog (ERROR, "cache lookup failed for type %u", constant->consttype);
        }
        typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
        ReleaseSysCache (tuple);
    
        /* the value must be "now" */
        if (strcmp (DatumGetCString (OidFunctionCall1 (typoutput, constant->constvalue)), "now") != 0)
          return NULL;
    
        initStringInfo (&result);
        switch (coerce->resulttype) {
          case DATEOID:
            appendStringInfo (&result, "TRUNC(CAST (CAST(?/*:now*/ AS TIMESTAMP) AS DATE))");
          break;
          case TIMESTAMPOID:
            appendStringInfo (&result, "(CAST (CAST (?/*:now*/ AS TIMESTAMP) AS TIMESTAMP))");
          break;
          case TIMESTAMPTZOID:
            appendStringInfo (&result, "(CAST (?/*:now*/ AS TIMESTAMP))");
          break;
          case TIMEOID:
            appendStringInfo (&result, "(CAST (CAST (?/*:now*/ AS TIME) AS TIME))");
          break;
          case TIMETZOID:
            appendStringInfo (&result, "(CAST (?/*:now*/ AS TIME))");
          break;
        }
      }
      break;
      #if PG_VERSION_NUM >= 100000
      case T_SQLValueFunction: {
        sqlvalfunc = (SQLValueFunction *) expr;
        switch (sqlvalfunc->op) {
          case SVFOP_CURRENT_DATE:
            initStringInfo (&result);
            appendStringInfo (&result, "TRUNC(CAST (CAST(?/*:now*/ AS TIMESTAMP) AS DATE))");
          break;
          case SVFOP_CURRENT_TIMESTAMP:
            initStringInfo (&result);
            appendStringInfo (&result, "(CAST (?/*:now*/ AS TIMESTAMP))");
          break;
          case SVFOP_LOCALTIMESTAMP:
            initStringInfo (&result);
            appendStringInfo (&result, "(CAST (CAST (?/*:now*/ AS TIMESTAMP) AS TIMESTAMP))");
          break;
          case SVFOP_CURRENT_TIME:
            initStringInfo (&result);
            appendStringInfo (&result, "(CAST (?/*:now*/ AS TIME))");
          break;
          case SVFOP_LOCALTIME:
            initStringInfo (&result);
            appendStringInfo (&result, "(CAST (CAST (?/*:now*/ AS TIME) AS TIME))");
          break;
          default:
            return NULL;		/* don't push down other functions */
        }
      }
      break;
      #endif
      default:
        /* we cannot translate this to DB2 */
        return NULL;
    }
  }
  db2Debug1("< %s::deparseExpr", __FILE__);
  return result.data;
}

/** datumToString
 *   Convert a Datum to a string by calling the type output function.
 *   Returns the result or NULL if it cannot be converted to DB2 SQL.
 */
char* datumToString (Datum datum, Oid type) {
  StringInfoData result;
  regproc        typoutput;
  HeapTuple      tuple;
  char*          str;
  char*          p;
  db2Debug1("> %s::datumToString", __FILE__);
  /* get the type's output function */
  tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (type));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for type %u", type);
  }
  typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
  ReleaseSysCache (tuple);

  /* render the constant in DB2 SQL */
  switch (type) {
    case TEXTOID:
    case CHAROID:
    case BPCHAROID:
    case VARCHAROID:
    case NAMEOID:
      str = DatumGetCString (OidFunctionCall1 (typoutput, datum));
      /*
       * Don't try to convert empty strings to DB2.
       * DB2 treats empty strings as NULL.
       */
      if (str[0] == '\0')
        return NULL;

      /* quote string */
      initStringInfo (&result);
      appendStringInfo (&result, "'");
      for (p = str; *p; ++p) {
        if (*p == '\'')
          appendStringInfo (&result, "'");
        appendStringInfo (&result, "%c", *p);
      }
      appendStringInfo (&result, "'");
    break;
    case INT8OID:
    case INT2OID:
    case INT4OID:
    case OIDOID:
    case FLOAT4OID:
    case FLOAT8OID:
    case NUMERICOID:
      str = DatumGetCString (OidFunctionCall1 (typoutput, datum));
      initStringInfo (&result);
      appendStringInfo (&result, "%s", str);
    break;
    case DATEOID:
      str = deparseDate (datum);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS DATE))", str);
    break;
    case TIMESTAMPOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIMESTAMP))", str);
    break;
    case TIMESTAMPTZOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIMESTAMP))", str);
    break;
    case TIMEOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIME))", str);
    break;
    case TIMETZOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIME))", str);
    break;
    case INTERVALOID:
      str = deparseInterval (datum);
      if (str == NULL)
        return NULL;
      initStringInfo (&result);
      appendStringInfo (&result, "%s", str);
    break;
    default:
      return NULL;
  }
  db2Debug1("< %s::datumToString - returns: '%s'", __FILE__, result.data);
  return result.data;
}

/** guessNlsLang
 *   If nls_lang is not NULL, return "NLS_LANG=<nls_lang>".
 *   Otherwise, return a good guess for DB2's NLS_LANG.
 */
char* guessNlsLang (char *nls_lang) {
  char *server_encoding, *lc_messages, *language = "AMERICAN_AMERICA", *charset = NULL;
  StringInfoData buf;
  db2Debug1("> %s::guessNlsLang(nls_lang: %s)", __FILE__, nls_lang);
  initStringInfo (&buf);
  if (nls_lang == NULL) {
    server_encoding = pstrdup (GetConfigOption ("server_encoding", false, true));
    /* find an DB2 client character set that matches the database encoding */
    if (strcmp (server_encoding, "UTF8") == 0)
      charset = "AL32UTF8";
    else if (strcmp (server_encoding, "EUC_JP") == 0)
      charset = "JA16EUC";
    else if (strcmp (server_encoding, "EUC_JIS_2004") == 0)
      charset = "JA16SJIS";
    else if (strcmp (server_encoding, "EUC_TW") == 0)
      charset = "ZHT32EUC";
    else if (strcmp (server_encoding, "ISO_8859_5") == 0)
      charset = "CL8ISO8859P5";
    else if (strcmp (server_encoding, "ISO_8859_6") == 0)
      charset = "AR8ISO8859P6";
    else if (strcmp (server_encoding, "ISO_8859_7") == 0)
      charset = "EL8ISO8859P7";
    else if (strcmp (server_encoding, "ISO_8859_8") == 0)
      charset = "IW8ISO8859P8";
    else if (strcmp (server_encoding, "KOI8R") == 0)
      charset = "CL8KOI8R";
    else if (strcmp (server_encoding, "KOI8U") == 0)
      charset = "CL8KOI8U";
    else if (strcmp (server_encoding, "LATIN1") == 0)
      charset = "WE8ISO8859P1";
    else if (strcmp (server_encoding, "LATIN2") == 0)
      charset = "EE8ISO8859P2";
    else if (strcmp (server_encoding, "LATIN3") == 0)
      charset = "SE8ISO8859P3";
    else if (strcmp (server_encoding, "LATIN4") == 0)
      charset = "NEE8ISO8859P4";
    else if (strcmp (server_encoding, "LATIN5") == 0)
      charset = "WE8ISO8859P9";
    else if (strcmp (server_encoding, "LATIN6") == 0)
      charset = "NE8ISO8859P10";
    else if (strcmp (server_encoding, "LATIN7") == 0)
      charset = "BLT8ISO8859P13";
    else if (strcmp (server_encoding, "LATIN8") == 0)
      charset = "CEL8ISO8859P14";
    else if (strcmp (server_encoding, "LATIN9") == 0)
      charset = "WE8ISO8859P15";
    else if (strcmp (server_encoding, "WIN866") == 0)
      charset = "RU8PC866";
    else if (strcmp (server_encoding, "WIN1250") == 0)
      charset = "EE8MSWIN1250";
    else if (strcmp (server_encoding, "WIN1251") == 0)
      charset = "CL8MSWIN1251";
    else if (strcmp (server_encoding, "WIN1252") == 0)
      charset = "WE8MSWIN1252";
    else if (strcmp (server_encoding, "WIN1253") == 0)
      charset = "EL8MSWIN1253";
    else if (strcmp (server_encoding, "WIN1254") == 0)
      charset = "TR8MSWIN1254";
    else if (strcmp (server_encoding, "WIN1255") == 0)
      charset = "IW8MSWIN1255";
    else if (strcmp (server_encoding, "WIN1256") == 0)
      charset = "AR8MSWIN1256";
    else if (strcmp (server_encoding, "WIN1257") == 0)
      charset = "BLT8MSWIN1257";
    else if (strcmp (server_encoding, "WIN1258") == 0)
      charset = "VN8MSWIN1258";
    else {
      /* warn if we have to resort to 7-bit ASCII */
      charset = "US7ASCII";
      ereport (WARNING,(errcode (ERRCODE_WARNING)
                      ,errmsg ("no DB2 character set for database encoding \"%s\"", server_encoding)
                      ,errdetail ("All but ASCII characters will be lost.")
                      ,errhint ("You can set the option \"%s\" on the foreign data wrapper to force an DB2 character set.", OPT_NLS_LANG)
                      )
              );
    }
    lc_messages = pstrdup (GetConfigOption ("lc_messages", false, true));
    /* try to guess those for which there is a backend translation */
    if (strncmp (lc_messages, "de_", 3) == 0 || pg_strncasecmp (lc_messages, "german", 6) == 0)
      language = "GERMAN_GERMANY";
    if (strncmp (lc_messages, "es_", 3) == 0 || pg_strncasecmp (lc_messages, "spanish", 7) == 0)
      language = "SPANISH_SPAIN";
    if (strncmp (lc_messages, "fr_", 3) == 0 || pg_strncasecmp (lc_messages, "french", 6) == 0)
      language = "FRENCH_FRANCE";
    if (strncmp (lc_messages, "in_", 3) == 0 || pg_strncasecmp (lc_messages, "indonesian", 10) == 0)
      language = "INDONESIAN_INDONESIA";
    if (strncmp (lc_messages, "it_", 3) == 0 || pg_strncasecmp (lc_messages, "italian", 7) == 0)
      language = "ITALIAN_ITALY";
    if (strncmp (lc_messages, "ja_", 3) == 0 || pg_strncasecmp (lc_messages, "japanese", 8) == 0)
      language = "JAPANESE_JAPAN";
    if (strncmp (lc_messages, "pt_", 3) == 0 || pg_strncasecmp (lc_messages, "portuguese", 10) == 0)
      language = "BRAZILIAN PORTUGUESE_BRAZIL";
    if (strncmp (lc_messages, "ru_", 3) == 0 || pg_strncasecmp (lc_messages, "russian", 7) == 0)
      language = "RUSSIAN_RUSSIA";
    if (strncmp (lc_messages, "tr_", 3) == 0 || pg_strncasecmp (lc_messages, "turkish", 7) == 0)
      language = "TURKISH_TURKEY";
    if (strncmp (lc_messages, "zh_CN", 5) == 0 || pg_strncasecmp (lc_messages, "chinese-simplified", 18) == 0)
      language = "SIMPLIFIED CHINESE_CHINA";
    if (strncmp (lc_messages, "zh_TW", 5) == 0 || pg_strncasecmp (lc_messages, "chinese-traditional", 19) == 0)
      language = "TRADITIONAL CHINESE_TAIWAN";
    appendStringInfo (&buf, "NLS_LANG=%s.%s", language, charset);
  } else {
    appendStringInfo (&buf, "NLS_LANG=%s", nls_lang);
  }
  db2Debug1("< %s::guessNlsLang - returns: '%s'", __FILE__, buf.data);
  return buf.data;
}

/** deparseDate
 *   Render a PostgreSQL date so that DB2 can parse it.
 */
char* deparseDate (Datum datum) {
  struct pg_tm   datetime_tm;
  StringInfoData s;
  db2Debug1("> %s::deparseDate", __FILE__);
  if (DATE_NOT_FINITE (DatumGetDateADT (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite date value cannot be stored in DB2")));

  /* get the parts */
  (void) j2date (DatumGetDateADT (datum) + POSTGRES_EPOCH_JDATE, &(datetime_tm.tm_year), &(datetime_tm.tm_mon), &(datetime_tm.tm_mday));

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  appendStringInfo (&s, "%04d-%02d-%02d 00:00:00", datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1, datetime_tm.tm_mon, datetime_tm.tm_mday);
  db2Debug1("< %s::deparseDate - returns: '%s'", __FILE__, s.data);
  return s.data;
}

/** deparseTimestamp
 *   Render a PostgreSQL timestamp so that DB2 can parse it.
 */
char* deparseTimestamp (Datum datum, bool hasTimezone) {
  struct pg_tm   datetime_tm;
  int32          tzoffset;
  fsec_t         datetime_fsec;
  StringInfoData s;
  db2Debug1("> %s::deparseTimestamp",__FILE__);
  /* this is sloppy, but DatumGetTimestampTz and DatumGetTimestamp are the same */
  if (TIMESTAMP_NOT_FINITE (DatumGetTimestampTz (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite timestamp value cannot be stored in DB2")));

  /* get the parts */
  tzoffset = 0;
  (void) timestamp2tm (DatumGetTimestampTz (datum), hasTimezone ? &tzoffset : NULL, &datetime_tm, &datetime_fsec, NULL, NULL);

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  if (hasTimezone)
    appendStringInfo (&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d%+03d:%02d",
      datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
      datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
      datetime_tm.tm_min, datetime_tm.tm_sec, (int32) datetime_fsec,
      -tzoffset / 3600, ((tzoffset > 0) ? tzoffset % 3600 : -tzoffset % 3600) / 60);
  else
    appendStringInfo (&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
      datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
      datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
      datetime_tm.tm_min, datetime_tm.tm_sec, (int32) datetime_fsec);
  db2Debug1("< %s::deparseTimestamp - returns: '%s'", __FILE__, s.data);
  return s.data;
}

/** deparsedeparseInterval
 *   Render a PostgreSQL timestamp so that DB2 can parse it.
 */
char* deparseInterval (Datum datum) {
  #if PG_VERSION_NUM >= 150000
  struct pg_itm tm;
  #else
  struct pg_tm tm;
  #endif
  fsec_t         fsec=0;
  StringInfoData s;
  char*          sign;
  int            idx = 0;

  db2Debug1("> %s::deparseInterval",__FILE__);
  #if PG_VERSION_NUM >= 150000
  interval2itm (*DatumGetIntervalP (datum), &tm);
  #else
  if (interval2tm (*DatumGetIntervalP (datum), &tm, &fsec) != 0) {
    elog (ERROR, "could not convert interval to tm");
  }
  #endif
  /* only translate intervals that can be translated to INTERVAL DAY TO SECOND */
//  if (tm.tm_year != 0 || tm.tm_mon != 0)
//    return NULL;

  /* DB2 intervals have only one sign */
  if (tm.tm_mday < 0 || tm.tm_hour < 0 || tm.tm_min < 0 || tm.tm_sec < 0 || fsec < 0) {
    sign = "-";
    /* all signs must match */
    if (tm.tm_mday > 0 || tm.tm_hour > 0 || tm.tm_min > 0 || tm.tm_sec > 0 || fsec > 0)
      return NULL;
    tm.tm_mday = -tm.tm_mday;
    tm.tm_hour = -tm.tm_hour;
    tm.tm_min  = -tm.tm_min;
    tm.tm_sec  = -tm.tm_sec;
    fsec       = -fsec;
  } else {
    sign = "+";
  }
  initStringInfo (&s);
  if (tm.tm_year > 0) {
    appendStringInfo(&s, ((tm.tm_year > 1) ? "%d YEARS" : "%d YEAR"),tm.tm_year);
  }
  idx += tm.tm_year;
  if (tm.tm_mon > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_mon > 1) ? "%d MONTHS" : "%d MONTH"),tm.tm_mon);
  }
  idx += tm.tm_mon;
  if (tm.tm_mday > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_mday > 1) ? "%d DAYS" : "%d DAY"),tm.tm_mday);
  }
  idx += tm.tm_mday;
  if (tm.tm_hour > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_hour > 1) ? "%ld HOURS" : "%ld HOUR"),tm.tm_hour);
  }
  idx += tm.tm_hour;
  if (tm.tm_min > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_min > 1) ? "%d MINUTES" : "%d MINUTE"),tm.tm_min);
  }
  idx += tm.tm_min;
  if (tm.tm_sec > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_sec > 1) ? "%d SECONDS" : "%d SECOND"),tm.tm_sec);
  }
  idx += tm.tm_sec;

//  #if PG_VERSION_NUM >= 150000
//  appendStringInfo (&s, "INTERVAL '%s%d %02ld:%02d:%02d.%06d' DAY TO SECOND", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
//  #else
//  appendStringInfo (&s, "INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY(9) TO SECOND(6)", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
//  #endif
  db2Debug1("< %s::deparseInterval - returns: '%s'",__FILE__,s.data);
  return s.data;
}

/** convertTuple
 *   Convert a result row from DB2 stored in db2Table
 *   into arrays of values and null indicators.
 *   If trunc_lob it true, truncate LOBs to WIDTH_THRESHOLD+1 bytes.
 */
void convertTuple (DB2FdwState* fdw_state, Datum* values, bool* nulls, bool trunc_lob) {
  char*                tmp_value = NULL;
  char*                value     = NULL;
  long                 value_len = 0;
  int                  j, 
                       index     = -1;
//  ErrorContextCallback errcb;
  Oid                  pgtype;

  db2Debug1("> %s::convertTuple",__FILE__);
  /* initialize error context callback, install it only during conversions */
//  errcb.callback = errorContextCallback;
//  errcb.arg = (void *) fdw_state;

  /* assign result values */
  for (j = 0; j < fdw_state->db2Table->npgcols; ++j) {
    short db2Type;
    db2Debug2("  start processing column %d of %d",j + 1, fdw_state->db2Table->npgcols);
    db2Debug2("  index: %d",index);
    /* for dropped columns, insert a NULL */
    if ((index + 1 < fdw_state->db2Table->ncols) && (fdw_state->db2Table->cols[index + 1]->pgattnum > j + 1)) {
      nulls[j] = true;
      values[j] = PointerGetDatum (NULL);
      continue;
    } else {
      ++index;
    }
    db2Debug2("  index: %d",index);
    /*
     * Columns exceeding the length of the DB2 table will be NULL,
     * as well as columns that are not used in the query.
     * Geometry columns are NULL if the value is NULL,
     * for all other types use the NULL indicator.
     */
    if (index >= fdw_state->db2Table->ncols || fdw_state->db2Table->cols[index]->used == 0 || fdw_state->db2Table->cols[index]->val_null == -1) {
      nulls[j] = true;
      values[j] = PointerGetDatum (NULL);
      continue;
    }

    /* from here on, we can assume columns to be NOT NULL */
    nulls[j] = false;
    pgtype = fdw_state->db2Table->cols[index]->pgtype;

    /* get the data and its length */
    switch(c2dbType(fdw_state->db2Table->cols[index]->colType)) {
      case DB2_BLOB:
      case DB2_CLOB: {
        db2Debug3("  DB2_BLOB or DB2CLOB");
        /* for LOBs, get the actual LOB contents (palloc'ed), truncated if desired */
        /* the column index is 1 based, whereas index id 0 based, so always add 1 to index when calling db2GetLob, since it does a column based access*/
        db2GetLob (fdw_state->session, fdw_state->db2Table->cols[index], index+1, &value, &value_len, trunc_lob ? (WIDTH_THRESHOLD + 1) : 0);
      }
      break;
      case DB2_LONGVARBINARY: {
        db2Debug3("  DB2_LONGBINARY datatypes");
        /* for LONG and LONG RAW, the first 4 bytes contain the length */
        value_len = *((int32 *) fdw_state->db2Table->cols[index]->val);
        /* the rest is the actual data */
        value = fdw_state->db2Table->cols[index]->val;
        /* terminating zero byte (needed for LONGs) */
        value[value_len] = '\0';
      }
      break;
      case DB2_FLOAT:
      case DB2_DECIMAL:
      case DB2_SMALLINT:
      case DB2_INTEGER:
      case DB2_REAL:
      case DB2_DECFLOAT:
      case DB2_DOUBLE: {
        db2Debug3("  DB2_FLOAT, DECIMAL, SMALLINT, INTEGER, REAL, DECFLOAT, DOUBLE");
        value     = fdw_state->db2Table->cols[index]->val;
        value_len = fdw_state->db2Table->cols[index]->val_len;
        value_len = (value_len == 0) ? strlen(value) : value_len;
        tmp_value = value;
        if((tmp_value = strchr(value,','))!=NULL) {
          *tmp_value = '.';
        }
      }
      break;
      default: {
        db2Debug3("  shoud be string based values");
        /* for other data types, db2Table contains the results */
        value     = fdw_state->db2Table->cols[index]->val;
        value_len = fdw_state->db2Table->cols[index]->val_len;
        value_len = (value_len == 0) ? strlen(value) : value_len;
      }
      break;
    }
    db2Debug2("  value    : '%x'", value);
    if (value != NULL) {
      db2Debug2("  value    : '%s'", value);
    }
    db2Debug2("  value_len: %ld" , value_len);
    db2Debug2("  fdw_state->db2Table->cols[%d]->val_null : %d",index,fdw_state->db2Table->cols[index]->val_len );
    db2Debug2("  fdw_state->db2Table->cols[%d]->val_null : %d",index,fdw_state->db2Table->cols[index]->val_null);
    db2Debug2("  fdw_state->db2Table->cols[%d]->pgname   : %s",index,fdw_state->db2Table->cols[index]->pgname  );
    db2Debug2("  fdw_state->db2Table->cols[%d]->pgattnum : %d",index,fdw_state->db2Table->cols[index]->pgattnum);
    db2Debug2("  fdw_state->db2Table->cols[%d]->pgtype   : %d",index,fdw_state->db2Table->cols[index]->pgtype  );
    db2Debug2("  fdw_state->db2Table->cols[%d]->pgtypemod: %d",index,fdw_state->db2Table->cols[index]->pgtypmod);
    /* fill the TupleSlot with the data (after conversion if necessary) */
    if (pgtype == BYTEAOID) {
      /* binary columns are not converted */
      bytea* result = (bytea*) palloc (value_len + VARHDRSZ);
      memcpy (VARDATA (result), value, value_len);
      SET_VARSIZE (result, value_len + VARHDRSZ);

      values[j] = PointerGetDatum (result);
    } else {
      regproc   typinput;
      HeapTuple tuple;
      Datum     dat;
      db2Debug2("  pgtype: %d",pgtype);
      /* find the appropriate conversion function */
      tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (pgtype));
      if (!HeapTupleIsValid (tuple)) {
        elog (ERROR, "cache lookup failed for type %u", pgtype);
      }
      typinput = ((Form_pg_type) GETSTRUCT (tuple))->typinput;
      ReleaseSysCache (tuple);
      db2Debug3("  CStringGetDatum");
      dat = CStringGetDatum (value);
      /* install error context callback */
//      db2Debug3("  error_context_stack");
//      db2Debug2("  errcb.previous: %x",errcb.previous);
//      errcb.previous         = error_context_stack;
//      db2Debug2("  errcb.previous: %x",errcb.previous);
//      db2Debug2("  &errcb: %x", &errcb);
//      error_context_stack    = &errcb;
      db2Debug2("  index: %d", index);
      fdw_state->columnindex = index;

      /* for string types, check that the data are in the database encoding */
      if (pgtype == BPCHAROID || pgtype == VARCHAROID || pgtype == TEXTOID) {
        db2Debug3("  pg_verify_mbstr");
        (void) pg_verify_mbstr (GetDatabaseEncoding (), value, value_len, fdw_state->db2Table->cols[index]->noencerr == NO_ENC_ERR_TRUE);
      }
      /* call the type input function */
      switch (pgtype) {
        case BPCHAROID:
        case VARCHAROID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case TIMEOID:
        case TIMETZOID:
        case INTERVALOID:
        case NUMERICOID:
          db2Debug3("  Calling OidFunctionCall3");
          /* these functions require the type modifier */
          values[j] = OidFunctionCall3 (typinput, dat, ObjectIdGetDatum (InvalidOid), Int32GetDatum (fdw_state->db2Table->cols[index]->pgtypmod));
          break;
        default:
          db2Debug3("  Calling OidFunctionCall1");
          /* the others don't */
          values[j] = OidFunctionCall1 (typinput, dat);
      }
      /* uninstall error context callback */
//      error_context_stack = errcb.previous;
    }

    /* free the data buffer for LOBs */
    db2Type = c2dbType(fdw_state->db2Table->cols[index]->colType);
    if (db2Type == DB2_BLOB || db2Type == DB2_CLOB) {
      db2Debug2("  value before pfree: %x",value);
      if (value != NULL) {
        pfree (value);
      } else {
        db2Debug2("  not freeing value, since it is null");
      }
    }
  }
  db2Debug1("< %s::convertTuple",__FILE__);
}

/** errorContextCallback
 *   Provides the context for an error message during a type input conversion.
 *   The argument must be a pointer to a DB2FdwState.
 */
void errorContextCallback (void* arg) {
  DB2FdwState *fdw_state = (DB2FdwState*) arg;
  db2Debug1("> %s::errorContextCallback",__FILE__);
  errcontext ( "converting column \"%s\" for foreign table scan of \"%s\", row %lu"
             , quote_identifier (fdw_state->db2Table->cols[fdw_state->columnindex]->pgname)
             , quote_identifier (fdw_state->db2Table->pgname)
             , fdw_state->rowcount
            );
  db2Debug1("< %s::errorContextCallback",__FILE__);
}

/** exitHook
 *   Close all DB2 connections on process exit.
 */
void exitHook (int code, Datum arg) {
  db2Debug1("> %s::exitHook",__FILE__);
  db2Shutdown ();
  db2Debug1("< %s::exitHook",__FILE__);
}
