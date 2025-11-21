#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */

/** external prototypes */
extern void      db2Debug1            (const char* message, ...);
extern void      db2Debug3            (const char* message, ...);
extern void      db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);

/** local prototypes */
void             db2CloseStatement    (DB2Session* session);

/** db2CloseStatement
 *   Close any open statement associated with the session.
 */
void db2CloseStatement (DB2Session* session) {
  db2Debug1("> db2CloseStatement");
  /* release statement handle, if it exists */
  if (session->stmtp != NULL) {
    /* release the statement handle */
    db2FreeStmtHdl(session->stmtp, session->connp);
    session->stmtp = NULL;
  } else {
    db2Debug3( "  no handle to close");
  }
  db2Debug1("< db2CloseStatement");
}
