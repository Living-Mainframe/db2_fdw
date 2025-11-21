#include <postgres.h>
#include <nodes/makefuncs.h>
#if PG_VERSION_NUM < 120000
#include <nodes/relation.h>
#include <optimizer/var.h>
#include <utils/tqual.h>
#else
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#endif
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void            db2CloseStatement         (DB2Session* session);
extern void            db2free                   (void* p);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2CleanupFdwState        (DB2FdwState* fdw_state);

/** local prototypes */
void db2EndForeignScan(ForeignScanState* node);

/** db2EndForeignScan
 *   Close the currently active DB2 statement.
 */
void db2EndForeignScan (ForeignScanState* node) {
  DB2FdwState* fdw_state = (DB2FdwState*) node->fdw_state;

  db2Debug1("> db2EndForeignScan");
  /* release the DB2 session */
  db2CloseStatement(fdw_state->session);
  // check fdw_state->session for dangling references that need to be freed
  db2free(fdw_state->session);
  fdw_state->session = NULL;
  // check fdw_state for dangling references that need to be freed
  db2CleanupFdwState(fdw_state);
  db2free(fdw_state);
  db2Debug1("< db2EndForeignScan");
}
