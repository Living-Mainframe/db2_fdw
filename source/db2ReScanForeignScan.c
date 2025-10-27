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
//#include "db2_pg.h"
#include "db2_fdw.h"
#include "ParamDesc.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void            db2CloseStatement         (DB2Session* session);
extern void            db2Debug1                 (const char* message, ...);

/** local prototypes */
void db2ReScanForeignScan(ForeignScanState* node);

/** db2ReScanForeignScan
 *   Close the DB2 statement if there is any.
 *   That causes the next db2IterateForeignScan call to restart the scan.
 */
void db2ReScanForeignScan (ForeignScanState* node) {
  DB2FdwState* fdw_state = (DB2FdwState*) node->fdw_state;
 
  db2Debug1("> db2ReScanForeignScan");
  /* close open DB2 statement if there is one */
  db2CloseStatement(fdw_state->session);
  /* reset row count to zero */
  fdw_state->rowcount = 0;
  db2Debug1("< db2ReScanForeignScan");
}
