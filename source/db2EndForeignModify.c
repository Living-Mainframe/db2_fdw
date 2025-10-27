#include <postgres.h>
#include <nodes/makefuncs.h>
#include <utils/memutils.h>
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

/** external variables */
extern bool     dml_in_transaction;
extern regproc* output_funcs;

/** external prototypes */
extern void            db2CloseStatement         (DB2Session* session);
extern void            db2Free                   (void* p);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2Debug2                 (const char* message, ...);

/** local prototypes */
void db2EndForeignModify(EState* estate, ResultRelInfo* rinfo);

/** db2EndForeignModify
 *   Close the currently active DB2 statement.
 */
void db2EndForeignModify (EState* estate, ResultRelInfo* rinfo) {
  DB2FdwState* fdw_state = (DB2FdwState*) rinfo->ri_FdwState;
  db2Debug1("> db2EndForeignModify");
  db2Debug2("  relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));
  MemoryContextDelete (fdw_state->temp_cxt);
  /* release the DB2 session */
  db2CloseStatement (fdw_state->session);
  db2Free(fdw_state->session);
  fdw_state->session = NULL;
  db2Debug1("< db2EndForeignModify");
}

