#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */

/** external prototypes */
extern void         db2Entry             (int level, const char* message, ...);
extern void         db2Exit              (int level, const char* message, ...);

/** local prototypes */
int                 db2IsStatementOpen   (DB2Session* session);

/* db2IsStatementOpen
 * Return 1 if there is a statement handle, else 0.
 */
int db2IsStatementOpen (DB2Session* session) {
  int result = 0;
  db2Entry(1,"> db2IsStatementOpen.c::db2IsStatementOpen");
  result = (session->stmtp != NULL && session->stmtp->hsql != SQL_NULL_HSTMT);
  db2Exit(1,"< db2IsStatementOpen.c::db2IsStatementOpen : %d",result);
  return result;
}
