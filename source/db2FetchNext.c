#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables  */

/** external variables */
extern char         db2Message[ERRBUFSIZE];/* contains DB2 error messages, set by db2CheckErr()             */
extern int          err_code;              /* error code, set by db2CheckErr()                              */

/** external prototypes */
extern void      db2Entry             (int level, const char* message, ...);
extern void      db2Exit              (int level, const char* message, ...);
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
int db2FetchNext (DB2Session* session);

/* db2FetchNext
 * Fetch the next result row, return 1 if there is one, else 0.
 */
int db2FetchNext (DB2Session* session) {
  SQLRETURN rc = 0;
  db2Entry(1,"> db2FetchNext.c::db2FetchNext");
  /* make sure there is a statement handle stored in "session" */
  if (session->stmtp == NULL) {
    db2Error (FDW_ERROR, "db2FetchNext internal error: statement handle is NULL");
  }
  /* fetch the next result row */
  rc = SQLFetchScroll (session->stmtp->hsql, SQL_FETCH_NEXT, 1);
  rc = db2CheckErr(rc, session->stmtp->hsql, session->stmtp->type, __LINE__, __FILE__);
  if (rc != SQL_SUCCESS && rc != SQL_NO_DATA) {
    db2Error_d (err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION, "error fetching result: SQLFetchScroll failed to fetch next result row", db2Message);
  }
  db2Exit(1,"< db2FetchNext.c::db2FetchNext : %d",(rc == SQL_SUCCESS));
  return (rc == SQL_SUCCESS);
}
