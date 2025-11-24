#include <postgres.h>
#if PG_VERSION_NUM >= 140000

#include <foreign/foreign.h>
#include <nodes/makefuncs.h>
#include <utils/guc.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/** external variables */

/** external prototypes */
extern void            db2Debug1                 (const char* message, ...);

/** local prototypes */
int db2_get_batch_size_option   (Relation rel);
int db2GetForeignModifyBatchSize(ResultRelInfo *rinfo);

/*
 * db2GetForeignModifyBatchSize
 *
 * Returns the batch size to use for INSERTs into this foreign table.
 *
 * Returning 1 tells the executor to not batch (i.e., call ExecForeignInsert
 * per-row). Any value > 1 enables batching, subject to what the executor
 * actually decides to send.
 */
int db2GetForeignModifyBatchSize(ResultRelInfo *rinfo) {
  Relation rel        = rinfo->ri_RelationDesc;
  int      batch_size = 0;

  db2Debug1("> db2GetForeignModifyBatchSize");
  /*
   * If the table has BEFORE/AFTER ROW triggers or a RETURNING clause
   * is involved, it’s safer to disable batching and just do per-row
   * inserts. That's what postgres_fdw does as well.:contentReference[oaicite:4]{index=4}
   */
  if (rel->trigdesc && (rel->trigdesc->trig_insert_before_row || rel->trigdesc->trig_insert_after_row)) {
    batch_size = 1;
  } else {
    /*
     * We don't have easy access to "has RETURNING" here like postgres_fdw
     * does (it stores that in its modify state), but PostgreSQL core
     * currently skips ExecForeignBatchInsert if there is a RETURNING
     * clause anyway.:contentReference[oaicite:5]{index=5}
     */
    batch_size = db2_get_batch_size_option(rel);
  }
  db2Debug1("< db2GetForeignModifyBatchSize - batch_size: %d", batch_size);
  return batch_size;
}

int db2_get_batch_size_option(Relation rel) {
  Oid            relid = RelationGetRelid(rel);
  ForeignTable*  table;
  ForeignServer* server;
  ListCell*      lc;
  int            batch_size = 0;

  db2Debug1("> db2_get_batch_size_option");
  table  = GetForeignTable(relid);
  server = GetForeignServer(table->serverid);

  /* Table-level option has precedence. */
  foreach(lc, table->options) {
    DefElem *def = (DefElem *) lfirst(lc);
    if (strcmp(def->defname, OPT_BATCH_SIZE) == 0) {
      char  *valstr = strVal(def->arg);
      int32  val;

      if (!parse_int(valstr, &val, 0, NULL) || val <= 0) {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                errmsg("invalid value for option \"%s\"", OPT_BATCH_SIZE),
                errdetail("Value must be a positive integer.")));
      }
      batch_size = val;
      break;
    }
  }

  if (batch_size < 1) {
    /* Fall back to server-level option, if any. */
    foreach(lc, server->options) {
      DefElem *def = (DefElem *) lfirst(lc);
      if (strcmp(def->defname, OPT_BATCH_SIZE) == 0) {
        char  *valstr = strVal(def->arg);
        int32  val;

        if (!parse_int(valstr, &val, 0, NULL) || val <= 0) {
          ereport(ERROR,
                  (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                   errmsg("invalid value for option \"%s\"", OPT_BATCH_SIZE),
                   errdetail("Value must be a positive integer.")));
        }
        batch_size = val;
        break;
      }
    }
  }

  /* Default: no batching */
  batch_size = (batch_size < 1) ? DEFAULT_BATCHSZ : batch_size; 
  db2Debug1("< db2_get_batch_size_option- batch_size: %d", batch_size);
  return batch_size;
}
#endif