#include <postgres.h>
#include <tcop/tcopprot.h>
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

/** external prototypes */
extern void         db2Cancel                 (void);
extern void         db2Debug1                 (const char* message, ...);

/** local prototypes */
void db2SetHandlers(void);
void db2Die        (SIGNAL_ARGS);

/** db2SetHandlers
 *   Set signal handler for SIGTERM.
 */
void db2SetHandlers (void) {
  pqsignal (SIGTERM, db2Die);
}

/** db2Die
 *   Terminate the current query and prepare backend shutdown.
 *   This is a signal handler function.
 */
void db2Die (SIGNAL_ARGS) {
  db2Debug1("> db2Die");
  /** Terminate any running queries.
   * The DB2 sessions will be terminated by exitHook().
   */
  db2Cancel();
  /** Call the original backend shutdown function.
   * If a query was canceled above, an error from DB2 would result.
   * To have the backend report the correct FATAL error instead,
   * we have to call CHECK_FOR_INTERRUPTS() before we report that error;
   * this is done in db2Error_d.
   */
  die (postgres_signal_arg);
  db2Debug1("< db2Die");
}