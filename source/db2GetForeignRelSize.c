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
extern DB2FdwState* db2GetFdwState               (Oid foreigntableid, double* sample_percent);
extern void         db2Debug1                 (const char* message, ...);
extern char*        deparseExpr               (DB2Session* session, RelOptInfo * foreignrel, Expr* expr, const DB2Table* db2Table, List** params);

/** local prototypes */
void  db2GetForeignRelSize  (PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
char* deparseWhereConditions(DB2FdwState* fdwState, RelOptInfo* baserel, List** local_conds, List** remote_conds);

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
  fdwState = db2GetFdwState(foreigntableid, NULL);
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
  fdwState->where_clause = deparseWhereConditions ( fdwState
                                                  , baserel
                                                  , &(fdwState->local_conds)
                                                  , &(fdwState->remote_conds)
                                                  );

  /* release DB2 session (will be cached) */
  pfree (fdwState->session);
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
  /* store the state so that the other planning functions can use it */
  baserel->fdw_private = (void *) fdwState;
  db2Debug1("< db2GetForeignRelSize");
}

/** deparseWhereConditions
 *   Classify conditions into remote_conds or local_conds.
 *   Those conditions that can be pushed down will be collected into
 *   an DB2 WHERE clause that is returned.
 */
char* deparseWhereConditions (DB2FdwState *fdwState, RelOptInfo * baserel, List ** local_conds, List ** remote_conds) {
  List*          conditions = baserel->baserestrictinfo;
  ListCell*      cell;
  char*          where;
  char*          keyword = "WHERE";
  StringInfoData where_clause;

  db2Debug1("> deparseWhereCondition");
  initStringInfo (&where_clause);
  foreach (cell, conditions) {
    /* check if the condition can be pushed down */
    where = deparseExpr (fdwState->session, baserel, ((RestrictInfo *) lfirst (cell))->clause, fdwState->db2Table, &(fdwState->params));
    if (where != NULL) {
      *remote_conds = lappend (*remote_conds, ((RestrictInfo *) lfirst (cell))->clause);

      /* append new WHERE clause to query string */
      appendStringInfo (&where_clause, " %s %s", keyword, where);
      keyword = "AND";
      pfree (where);
    } else {
      *local_conds = lappend (*local_conds, ((RestrictInfo *) lfirst (cell))->clause);
    }
  }
  db2Debug1("< deparseWhereCondition");
  return where_clause.data;
}
