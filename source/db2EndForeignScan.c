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
#include "ParamDesc.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void            db2CloseStatement         (DB2Session* session);
extern void            db2Free                   (void* p);
extern void            db2Debug1                 (const char* message, ...);

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
  db2Free(fdw_state->session);
  fdw_state->session = NULL;
  db2Debug1("< db2EndForeignScan");
}
