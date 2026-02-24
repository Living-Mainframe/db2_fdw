#include <string.h>
#include <stdio.h>
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"
#include "ParamDesc.h"

/** global variables */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void*        db2alloc             (const char* type, size_t size);
extern void         db2Entry             (int level, const char* message, ...);
extern void         db2Exit              (int level, const char* message, ...);
extern void         db2Debug             (int level, const char* message, ...);
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLSMALLINT  param2c              (SQLSMALLINT fcType);
extern void         parse2num_struct     (const char* s, SQL_NUMERIC_STRUCT* ns);
extern char*        c2name               (short fcType);

/** internal prototypes */
void db2BindParameter (DB2Session* session, ParamDesc* param, SQLLEN* indicator, int param_count, int col_num);

void db2BindParameter (DB2Session* session, ParamDesc* param, SQLLEN* indicator, int param_count, int col_num) {
  SQLRETURN   rc           = 0;
  db2Entry(1,"> db2BindParameter.c::db2BindParameter");
  db2Debug(2,"param_count     : %d",param_count);
  db2Debug(2,"col_num         : %d",col_num);
  db2Debug(2,"param->value    : %s",param->value);
  db2Debug(2,"param->colnum   : %d",param->colnum);
  db2Debug(2,"param->bindType : %d",param->bindType);
  if (param->colnum >= 0) {
    db2Debug(2,"colName         : %s",param->colName);
  }
  switch (param->bindType) {
      case BIND_NUMBER: {
        db2Debug(3,"param->bindType: BIND_NUMBER");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : 0);
        db2Debug(2,"param_ind       : %d",*indicator);
        db2Debug(2,"colType         : %d - %s",param->colType,c2name(param->colType));
        switch (param->colType) {
          case SQL_BIGINT:{
            char*      end     = NULL;
            SQLBIGINT* sqlbint = NULL;
            if (param->value != NULL) {
              sqlbint  = db2alloc("SQLBIGINT",sizeof(SQLBIGINT));
              *sqlbint = strtoll(param->value,&end,10);
              db2Debug(2,"sqlbint: %d",*sqlbint);
            }
            rc = SQLBindParameter( session->stmtp->hsql
                                 , col_num
                                 , SQL_PARAM_INPUT
                                 , SQL_C_SBIGINT
                                 , param->colType
                                 , 0
                                 , 0
                                 , sqlbint
                                 , 0
                                 , indicator
                                 );
          }
          break;
          case SQL_SMALLINT:{
            char*        end     = NULL;
            SQLSMALLINT* sqlsint = NULL;
            if (param->value != NULL) {
              sqlsint  = db2alloc("SQLSMALLINT",sizeof(SQLSMALLINT));
              *sqlsint = strtol(param->value,&end,10);
              db2Debug(2,"sqlsint: %d",*sqlsint);
            }
            rc = SQLBindParameter( session->stmtp->hsql
                                 , col_num
                                 , SQL_PARAM_INPUT
                                 , SQL_C_SSHORT
                                 , param->colType
                                 , 0
                                 , 0
                                 , sqlsint
                                 , 0
                                 , indicator
                                 );
          }
          break;
          case SQL_INTEGER: {
            char*       end    = NULL;
            SQLINTEGER* sqlint = NULL;
            if (param->value != NULL) {
              sqlint  = db2alloc("SQLINTEGER",sizeof(SQLINTEGER));
              *sqlint = strtol(param->value,&end,10);
              db2Debug(2,"sqlint: %d",*sqlint);
            }
            rc = SQLBindParameter( session->stmtp->hsql
                                 , col_num
                                 , SQL_PARAM_INPUT
                                 , SQL_C_SLONG
                                 , param->colType
                                 , 0
                                 , 0
                                 , sqlint
                                 , 0
                                 , indicator
                                 );
          }
          break;
          case SQL_DECIMAL:
          case SQL_NUMERIC:
          case SQL_FLOAT:
          case SQL_REAL:
          case SQL_DOUBLE:
          case SQL_DECFLOAT: {
            SQL_NUMERIC_STRUCT* num = NULL;
            if (param->value != NULL) {
              num = db2alloc("SQL_NUMERIC_STRUCT",sizeof(SQL_NUMERIC_STRUCT));
              parse2num_struct(param->value, num);
              db2Debug(2,"num: '%s'",*num);
            }
            rc = SQLBindParameter( session->stmtp->hsql
                                 , col_num
                                 , SQL_PARAM_INPUT
                                 , SQL_C_NUMERIC
                                 , param->colType
                                 , num->precision
                                 , num->scale
                                 , num
                                 , sizeof(*num)
                                 , indicator
                                 );
          }
          break;
          default: {
            snprintf(db2Message, ERRBUFSIZE, "unsupported sql number type: %d - %s" , param->colType, c2name(param->colType)); 
            db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing isrt query: unable to bind parameter", db2Message);
          }
          break;
        }
      }
      break;
      case BIND_STRING: {
        db2Debug(3,"param->bindType: BIND_STRING");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug(2,"param_ind       : %d",*indicator);
        rc = SQLBindParameter( session->stmtp->hsql
                             , col_num
                             , SQL_PARAM_INPUT
                             , SQL_C_CHAR
                             , SQL_VARCHAR
                             , param->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , indicator
                             );
      }
      break;
      case BIND_LONGRAW: {
        db2Debug(3,"param->bindType: BIND_LONGRAW");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug(2,"param_ind       : %d",*indicator);
        rc = SQLBindParameter( session->stmtp->hsql
                             , col_num
                             , SQL_PARAM_INPUT
                             , SQL_C_BINARY
                             , SQL_LONGVARBINARY
                             , param->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , indicator
                             );
      }
      break;
      case BIND_LONG: {
        db2Debug(3,"param->bindType: BIND_LONG");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : SQL_NTS);
        db2Debug(2,"param_ind       : %d",*indicator);
        db2Debug(2,"param->value    : '%s'",param->value);
        rc = SQLBindParameter( session->stmtp->hsql
                             , col_num
                             , SQL_PARAM_INPUT
                             , SQL_C_CHAR
                             , SQL_LONGVARCHAR
                             , param->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , 0
                             , indicator
                             );
      }
      break;
      case BIND_OUTPUT: {
        SQLSMALLINT fcType;
        SQLSMALLINT fParamType;
        db2Debug(2,"param->bindType: BIND_OUTPUT");
        *indicator = (SQLLEN) ((param->value == NULL) ? SQL_NULL_DATA : 0);
        db2Debug(2,"param_ind       : %d",*indicator);
        if (param->type == UUIDOID) {
          /* the type input function will interpret the string value correctly */
          fcType = SQL_CHAR;
        } else {
          fcType = param->colType;
        }
        fParamType = param2c(fcType);
        rc = SQLBindParameter( session->stmtp->hsql
                             , param_count
                             , SQL_PARAM_OUTPUT
                             , fParamType
                             , fcType
                             , param->colSize
                             , 0
                             , (SQLPOINTER) param->value
                             , param->val_size
                             , indicator
                             );
      }
      break;
  }
  /* bind the value to the parameter */
  rc = db2CheckErr(rc,  session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d(FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLBindParameter failed to bind parameter", db2Message);
  }
  db2Exit(1,"< db2BindParameter.c::db2BindParameter");
}