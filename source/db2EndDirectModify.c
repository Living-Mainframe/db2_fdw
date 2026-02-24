#include <postgres.h>
#include "db2_fdw.h"
#include "DB2FdwDirectModifyState.h"

/** external variables */
extern regproc* output_funcs;

/** external prototypes */
extern void         db2CloseStatement    (DB2Session* session);
extern void         db2free              (void* p);
extern void         db2Entry             (int level, const char* message, ...);
extern void         db2Exit              (int level, const char* message, ...);
extern void         db2Debug             (int level, const char* message, ...);

/** local prototypes */
void db2EndDirectModify(ForeignScanState* node);

/* postgresEndDirectModify
 * Finish a direct foreign table modification
 */
void db2EndDirectModify(ForeignScanState* node) {
	DB2FdwDirectModifyState* fdw_state = (DB2FdwDirectModifyState*) node->fdw_state;

	db2Entry(1,"> db2EndDirectModify.c::db2EndDirectModify");
	/* MemoryContext will be deleted automatically. */
  if (fdw_state == NULL) {
    db2Debug(2,"no fdw_state, nothing to do");
    return;
  }

//  /* If you’re batching for COPY, flush any remaining rows here */
//  if (fdw_state->session && fdw_state->db2Table) {
//    /* e.g. db2FlushBatch(fdw_state->session, fdw_state->db2Table); */
//  }

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
 
  db2free(fdw_state);
  node->fdw_state = NULL;
	db2Exit(1,"< db2EndDirectModify.c::db2EndDirectModify");
}