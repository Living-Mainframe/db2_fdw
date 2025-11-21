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
extern void         db2Debug1            (const char* message, ...);
extern void         db2Debug2            (const char* message, ...);
extern SQLRETURN    db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);
extern void         db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);

/** internal prototypes */
int                 db2ExecuteTruncate   (DB2Session* session);

/** db2ExecuteTruncate
 */
int db2ExecuteTruncate   (DB2Session* session) {
  SQLRETURN   rc           = 0;
  SQLINTEGER  rowcount_val = 0;
  int         rowcount     = 0;
  
  db2Debug1("> db2ExecuteTruncate");
  /* execute the query and get the first result row */

  /* Execute TRUNCATE */
  rc = SQLExecute (session->stmtp->hsql);
//  rc = SQLExecDirect(session->stmtp->hsql, (SQLCHAR*) query, SQL_NTS);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    /* use the correct SQLSTATE for serialization failures */
    db2Error_d(err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLExecute failed to execute remote query", db2Message);
  }

  /* get the number of processed rows (important for DML) */
  rc = SQLRowCount(session->stmtp->hsql, &rowcount_val);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS) {
    db2Error_d ( FDW_UNABLE_TO_CREATE_EXECUTION, "error executing query: SQLRowCount failed to get number of affected rows", db2Message);
  }
  db2Debug2("  rowcount_val: %lld", rowcount_val);
  rowcount = (int) rowcount_val;
  db2Debug1("< db2ExecuteTruncate - returns: %d",rowcount);
  return rowcount;
}
