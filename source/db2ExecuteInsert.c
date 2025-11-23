#include <string.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"
#include "ParamDesc.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern void*        db2alloc             (const char* type, size_t size);
extern void         db2free              (void* p);
extern void         db2Debug1            (const char* message, ...);
extern void         db2Debug2            (const char* message, ...);
extern void         db2Debug3            (const char* message, ...);
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLSMALLINT  param2c              (SQLSMALLINT fcType);
extern void         parse2num_struct     (const char* s, SQL_NUMERIC_STRUCT* ns);

/** internal prototypes */
int                 db2ExecuteInsert     (DB2Session* session, const DB2Table* db2Table, ParamDesc* paramList);

/** db2ExecuteInsert
 *   Execute a prepared statement and fetches the first result row.
 *   The parameters ("bind variables") are filled from paramList.
 *   Returns the count of processed rows.
 *   This can be called several times for a prepared SQL statement.
 */
int db2ExecuteInsert (DB2Session* session, const DB2Table* db2Table, ParamDesc* paramList) {
  SQLLEN*     indicators   = NULL;
  ParamDesc*  param        = NULL;
  SQLRETURN   rc           = 0;
  SQLINTEGER  rowcount_val = 0;
  SQLSMALLINT outlen       = 0;
  SQLCHAR     cname[256]   = {0};  /* 256 is usually plenty; see note below */
  int         rowcount     = 0;
  int         param_count  = 0;
  
  db2Debug1("> db2ExecuteInsert");
  for (param = paramList; param != NULL; param = param->next) {
    ++param_count;
  }
  db2Debug2("  paramcount: %d",param_count);
  /* allocate a temporary array of indicators */
  indicators = db2alloc ("indicators", param_count * sizeof (SQLLEN));

  /* bind the parameters */
  param_count = 0;
  for (param = paramList; param; param = param->next) {
    ++param_count;
    db2Debug2("  paramcount      : %d",param_count);
    db2Debug2("  param->value    : %s",param->value);
    db2Debug2("  param->colnum   : %d",param->colnum);
    db2Debug2("  param->bindType : %d",param->bindType);
    db2Debug2("  param_count     : %d",param_count);
    db2Debug2("  colName         : %s",db2Table->cols[param->colnum]->colName);
    switch (param->bindType) {
      case BIND_NUMBER: {
        db2Debug3("  param->bindType: BIND_NUMBER");
        indicators[param_count] = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : 0);
        db2Debug2("  param_ind       : %d",indicators[param_count]);
        switch (db2Table->cols[param->colnum]->colType){
          case SQL_SMALLINT:{
            char* end = NULL;
            SQLSMALLINT sqlint = strtol(param->value,&end,10);
            db2Debug2("  sqlint: %d",sqlint);
            db2Debug2("  param->bindType: SQL_SMALLINT");
            rc = SQLBindParameter( session->stmtp->hsql
                                 , param->colnum+1
                                 , SQL_PARAM_INPUT
                                 , SQL_C_SSHORT
                                 , db2Table->cols[param->colnum]->colType
                                 , 0
                                 , 0
                                 , &sqlint
                                 , 0
                                 , &indicators[param_count]
                                 );
          }
          break;
          case SQL_INTEGER: {
            char* end = NULL;
            SQLINTEGER sqlint = strtol(param->value,&end,10);
            db2Debug2("  sqlint: %d",sqlint);
            db2Debug2("  param->bindType: SQL_INTEGER");
            rc = SQLBindParameter( session->stmtp->hsql
                                 , param->colnum+1
                                 , SQL_PARAM_INPUT
                                 , SQL_C_SLONG
                                 , db2Table->cols[param->colnum]->colType
                                 , 0
                                 , 0
                                 , &sqlint
                                 , 0
                                 , &indicators[param_count]
                                 );
          }
          break;
          default: {
            SQL_NUMERIC_STRUCT num = {0};
            parse2num_struct(param->value, &num);
            db2Debug2("  param->bindType: SQL_NUMERIC");
            db2Debug2("  num: '%s'",num);
            rc = SQLBindParameter( session->stmtp->hsql
                                 , param->colnum+1
                                 , SQL_PARAM_INPUT
                                 , SQL_C_NUMERIC
                                 , db2Table->cols[param->colnum]->colType
                                 , num.precision
                                 , num.scale
                                 , &num
                                 , sizeof(num)
                                 , &indicators[param_count]
                                 );
          }
          break;
        }
      }
      break;
      case BIND_STRING: {
        db2Debug3("  param->bindType: BIND_STRING");
        indicators[param_count] = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug2("  param_ind       : %d",indicators[param_count]);
        rc = SQLBindParameter( session->stmtp->hsql
                             , param->colnum+1
                             , SQL_PARAM_INPUT
                             , SQL_C_CHAR
                             , SQL_VARCHAR
                             , db2Table->cols[param->colnum]->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , &indicators[param_count]
                             );
      }
      break;
      case BIND_LONGRAW: {
        db2Debug3("  param->bindType: BIND_LONGRAW");
        indicators[param_count] = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug2("  param_ind       : %d",indicators[param_count]);
        rc = SQLBindParameter( session->stmtp->hsql
                             , param->colnum+1
                             , SQL_PARAM_INPUT
                             , SQL_C_BINARY
                             , SQL_LONGVARBINARY
                             , db2Table->cols[param->colnum]->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , &indicators[param_count]
                             );
      }
      break;
      case BIND_LONG: {
        db2Debug3("  param->bindType: BIND_LONG");
        indicators[param_count] = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug2("  param_ind       : %d",indicators[param_count]);
        db2Debug2("  param->value    : '%s'",param->value);
        rc = SQLBindParameter( session->stmtp->hsql
                             , param->colnum+1
                             , SQL_PARAM_INPUT
                             , SQL_C_CHAR
                             , SQL_LONGVARCHAR
                             , db2Table->cols[param->colnum]->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , &indicators[param_count]
                             );
      }
      break;
      case BIND_OUTPUT: {
        SQLSMALLINT fcType;
        SQLSMALLINT fParamType;
        db2Debug2("  param->bindType: BIND_OUTPUT");
        indicators[param_count] = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : 0);
        db2Debug2("  param_ind       : %d",indicators[param_count]);
        if (db2Table->cols[param->colnum]->pgtype == UUIDOID) {
          /* the type input function will interpret the string value correctly */
          fcType = SQL_CHAR;
        } else {
          fcType = db2Table->cols[param->colnum]->colType;
        }
        fParamType = param2c(fcType);
        rc = SQLBindParameter( session->stmtp->hsql
                             , param_count
                             , SQL_PARAM_OUTPUT
                             , fParamType
                             , fcType
                             , db2Table->cols[param->colnum]->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , db2Table->cols[param->colnum]->val_size
                             , &indicators[param_count]
                             );
      }
      break;
    }
    /* bind the value to the parameter */
    rc = db2CheckErr(rc,  session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
    if (rc != SQL_SUCCESS) {
      db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLBindParameter failed to bind parameter", db2Message);
    }
  }
  /* execute the query and get the first result row */
  db2Debug2("  session->stmtp->hsql: %d",session->stmtp->hsql);
  rc = SQLGetCursorName(session->stmtp->hsql, cname, (SQLSMALLINT)sizeof(cname), &outlen); 
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLGetCusorName failed to obtain cursor name", db2Message);
  }
  db2Debug2("  cursor name: '%s'", cname);
  rc = SQLExecute (session->stmtp->hsql);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    /* use the correct SQLSTATE for serialization failures */
    db2Error_d(err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLExecute failed to execute remote query", db2Message);
  }
  /* db2free all indicators */
  db2free (indicators);
  if (rc == SQL_NO_DATA) {
    db2Debug3("  SQL_NO_DATA");
    db2Debug1("< db2ExecuteInsert - returns: 0");
    return 0;
  }

  /* get the number of processed rows (important for DML) */
  rc = SQLRowCount(session->stmtp->hsql, &rowcount_val);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLRowCount failed to get number of affected rows", db2Message);
  }
  db2Debug2("  rowcount_val: %lld", rowcount_val);
  rowcount = (int) rowcount_val;
  db2Debug1("< db2ExecuteInsert - returns: %d",rowcount);
  return rowcount;
}
