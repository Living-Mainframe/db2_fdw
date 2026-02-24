#include <postgres.h>
#include <nodes/makefuncs.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2CloseStatement         (DB2Session* session);
extern void         db2Entry                  (int level, const char* message, ...);
extern void         db2Exit                   (int level, const char* message, ...);

/** local prototypes */
void db2ReScanForeignScan(ForeignScanState* node);

/* db2ReScanForeignScan
 * Close the DB2 statement if there is any.
 * That causes the next db2IterateForeignScan call to restart the scan.
 */
void db2ReScanForeignScan (ForeignScanState* node) {
  DB2FdwState* fdw_state = (DB2FdwState*) node->fdw_state;
 
  db2Entry(1,"> db2ReScanForeignScan.c::db2ReScanForeignScan");
  /* close open DB2 statement if there is one */
  db2CloseStatement(fdw_state->session);
  /* reset row count to zero */
  fdw_state->rowcount = 0;
  db2Exit(1,"< db2ReScanForeignScan.c::db2ReScanForeignScan");
}
