#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */

/** external prototypes */
extern void          db2Debug1            (const char* message, ...);

/** local prototypes */
int                  db2IsStatementOpen   (DB2Session* session);

/** db2IsStatementOpen
 *   Return 1 if there is a statement handle, else 0.
 */
int db2IsStatementOpen (DB2Session* session) {
  int result = 0;
  db2Debug1("> db2IsStatementOpen");
  result = (session->stmtp != NULL && session->stmtp->hsql != SQL_NULL_HSTMT);
  db2Debug1("< db2IsStatementOpen - result: %d",result);
  return result;
}
