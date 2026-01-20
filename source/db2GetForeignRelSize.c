#include <postgres.h>
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
extern DB2FdwState* db2GetFdwState            (Oid foreigntableid, double* sample_percent, bool describe);
extern void         db2Debug1                 (const char* message, ...);
extern char*        deparseWhereConditions    (PlannerInfo* root, RelOptInfo* baserel);
extern void         db2free                   (void* p);

/** local prototypes */
void  db2GetForeignRelSize  (PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);

/** db2GetForeignRelSize
 *   Get an DB2FdwState for this foreign scan.
 *   Construct the remote SQL query.
 *   Provide estimates for the number of tuples, the average width and the cost.
 */
void db2GetForeignRelSize (PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid) {
  DB2FdwState* fdwState = NULL;
  int          i        = 0;
  double       ntuples  = -1;

  db2Debug1("> db2GetForeignRelSize");
  /* get connection options, connect and get the remote table description */
  fdwState = db2GetFdwState(foreigntableid, NULL, true);
  /* store the state so that the other planning functions can use it */
  baserel->fdw_private = (void *) fdwState;
  /** Store the table OID in each table column.
   * This is redundant for base relations, but join relations will
   * have columns from different tables, and we have to keep track of them.
   */
  for (i = 0; i < fdwState->db2Table->ncols; ++i) {
    fdwState->db2Table->cols[i]->varno = baserel->relid;
  }
  /** Classify conditions into remote_conds or local_conds.
   * These parameters are used in foreign_join_ok and db2GetForeignPlan.
   * Those conditions that can be pushed down will be collected into
   * an DB2 WHERE clause.
   */
  fdwState->where_clause = deparseWhereConditions ( root, baserel );

  /* release DB2 session (will be cached) */
  db2free (fdwState->session);
  fdwState->session = NULL;
  /* use a random "high" value for cost */
  fdwState->startup_cost = 10000.0;
  /* if baserel->pages > 0, there was an ANALYZE; use the row count estimate */
  if (baserel->pages > 0)
    ntuples = baserel->tuples;
  /* estimale selectivity locally for all conditions */
  /* apply statistics only if we have a reasonable row count estimate */
  if (ntuples != -1) {
    /* estimate how conditions will influence the row count */
    ntuples = ntuples * clauselist_selectivity (root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);
    /* make sure that the estimate is not less that 1 */
    ntuples = clamp_row_est (ntuples);
    baserel->rows = ntuples;
  }
  /* estimate total cost as startup cost + 10 * (returned rows) */
  fdwState->total_cost = fdwState->startup_cost + baserel->rows * 10.0;
  db2Debug1("< db2GetForeignRelSize");
}
