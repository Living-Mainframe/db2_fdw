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
#include "db2_fdw.h"
#include "DB2FdwState.h"
/** external variables */
extern regproc* output_funcs;

/** external prototypes */
extern void         db2CloseStatement    (DB2Session* session);
extern void         db2free              (void* p);
extern void         db2Debug1            (const char* message, ...);
extern void         db2Debug2            (const char* message, ...);

/** local prototypes */
void                db2EndForeignModifyCommon(EState *estate, ResultRelInfo *rinfo);

void db2EndForeignModifyCommon(EState *estate, ResultRelInfo *rinfo) {
  DB2FdwState *fdw_state = NULL;

  db2Debug1("> db2EndForeignModifyCommon");
  db2Debug2("  relid: %d", RelationGetRelid (rinfo->ri_RelationDesc));

  fdw_state = (DB2FdwState*) rinfo->ri_FdwState;
  if (fdw_state == NULL) {
    db2Debug2("  no fdw_state, nothing to do");
    return;
  }

  /* If you’re batching for COPY, flush any remaining rows here */
  if (fdw_state->session && fdw_state->db2Table) {
        /* e.g. db2FlushBatch(fdw_state->session, fdw_state->db2Table); */
  }

  /* Finish statement / cursor, if you keep a handle there */
  if (fdw_state->session) {
    db2CloseStatement (fdw_state->session);
    db2free(fdw_state->session);
    fdw_state->session = NULL;
  }

  if (fdw_state->temp_cxt) {
    MemoryContextDelete (fdw_state->temp_cxt);
    fdw_state->temp_cxt = NULL;
  }

  if (output_funcs){
    db2free(output_funcs);
  }

  rinfo->ri_FdwState = NULL;
  db2Debug1("< db2EndForeignModifyCommon");
}
