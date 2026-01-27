#include <postgres.h>
#include <optimizer/planmain.h>
#include <optimizer/restrictinfo.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/* This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex {
  FdwPathPrivateHasFinalSort, /* has-final-sort flag (as a Boolean node) */
  FdwPathPrivateHasLimit,     /* has-limit flag (as a Boolean node)      */
};

/** external prototypes */
extern void   db2Debug1               (const char* message, ...);
extern void   db2Debug2               (const char* message, ...);
extern void   db2Debug3               (const char* message, ...);
extern bool   is_foreign_expr         (PlannerInfo* root, RelOptInfo* baserel, Expr* expr);
extern void   deparseSelectStmtForRel (StringInfo buf, PlannerInfo* root, RelOptInfo* rel, List* tlist, List* remote_conds, List* pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List** retrieved_attrs, List** params_list);
extern List*  build_tlist_to_deparse  (RelOptInfo* foreignrel);
extern List*  serializePlanData       (DB2FdwState* fdw_state);

/** local prototypes */
ForeignScan*  db2GetForeignPlan       (PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses , Plan* outer_plan);

/* postgresGetForeignPlan
 * Create ForeignScan plan node which implements selected best path
 */
ForeignScan* db2GetForeignPlan(PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses, Plan* outer_plan) {
  DB2FdwState*   fpinfo            = (DB2FdwState*) foreignrel->fdw_private;
  ForeignScan*   fscan             = NULL;
  List*          fdw_private       = NIL;
  List*          remote_exprs      = NIL;
  List*          local_exprs       = NIL;
  List*          params_list       = NIL;
  List*          fdw_scan_tlist    = NIL;
  List*          fdw_recheck_quals = NIL;
  List*          retrieved_attrs   = NIL;
  ListCell*      lc                = NULL;
  bool           has_final_sort    = false;
  bool           has_limit         = false;
  Index          scan_relid;
  StringInfoData sql;

  db2Debug1("> %s::db2GetForeignPlan",__FILE__);
  /* Get FDW private data created by db2GetForeignUpperPaths(), if any. */
  if (best_path->fdw_private) {
    has_final_sort  = boolVal(list_nth(best_path->fdw_private, FdwPathPrivateHasFinalSort));
    has_limit       = boolVal(list_nth(best_path->fdw_private, FdwPathPrivateHasLimit));
  }

  if (IS_SIMPLE_REL(foreignrel)) {
    /* For base relations, set scan_relid as the relid of the relation. */
    scan_relid = foreignrel->relid;

    /* In a base-relation scan, we must apply the given scan_clauses.
     *
     * Separate the scan_clauses into those that can be executed remotely and those that can't.
     * baserestrictinfo clauses that were previously determined to be safe or unsafe by classifyConditions
     * are found in fpinfo->remote_conds and fpinfo->local_conds.
     * Anything else in the scan_clauses list will be a join clause, which we have to check for remote-safety.
     *
     * Note: the join clauses we see here should be the exact same ones previously examined by postgresGetForeignPaths.
     * Possibly it'd be worth passing forward the classification work done then, rather than repeating it here.
     *
     * This code must match "extract_actual_clauses(scan_clauses, false)" except for the additional decision about remote versus local execution.
     */
    foreach(lc, scan_clauses) {
      RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

      /* Ignore any pseudoconstants, they're dealt with elsewhere */
      if (rinfo->pseudoconstant)
        continue;

      if (list_member_ptr(fpinfo->remote_conds, rinfo))
        remote_exprs = lappend(remote_exprs, rinfo->clause);
      else if (list_member_ptr(fpinfo->local_conds, rinfo))
        local_exprs = lappend(local_exprs, rinfo->clause);
      else if (is_foreign_expr(root, foreignrel, rinfo->clause))
        remote_exprs = lappend(remote_exprs, rinfo->clause);
      else
        local_exprs = lappend(local_exprs, rinfo->clause);
    }

    /* For a base-relation scan, we have to support EPQ recheck, which should recheck all the remote quals. */
    fdw_recheck_quals = remote_exprs;
  } else {
    /* Join relation or upper relation - set scan_relid to 0. */
    scan_relid = 0;

    /* For a join rel, baserestrictinfo is NIL and we are not considering parameterization right now, 
     * so there should be no scan_clauses for a joinrel or an upper rel either.
     */
    Assert(!scan_clauses);

    /* Instead we get the conditions to apply from the fdw_private structure. */
    remote_exprs  = extract_actual_clauses(fpinfo->remote_conds, false);
    local_exprs   = extract_actual_clauses(fpinfo->local_conds, false);

    /* We leave fdw_recheck_quals empty in this case, since we never need to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
     * recheck is handled elsewhere --- see postgresGetForeignJoinPaths().
     * If we're planning an upperrel (ie, remote grouping or aggregation) then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
     * allowed, and indeed we *can't* put the remote clauses into fdw_recheck_quals because the unaggregated Vars won't be available
     * locally.
     * 
     * Build the list of columns to be fetched from the foreign server.
     */
    fdw_scan_tlist = build_tlist_to_deparse(foreignrel);

    /* Ensure that the outer plan produces a tuple whose descriptor matches our scan tuple slot.  Also, remove the local conditions
     * from outer plan's quals, lest they be evaluated twice, once by the local plan and once by the scan.
     */
    if (outer_plan) {
      /* Right now, we only consider grouping and aggregation beyond joins. 
       * Queries involving aggregates or grouping do not require EPQ mechanism, hence should not have an outer plan here.
       */
      Assert(!IS_UPPER_REL(foreignrel));
      /* First, update the plan's qual list if possible.
       * In some cases the quals might be enforced below the topmost plan level, in which case we'll fail to remove them; it's not worth working
       * harder than this.
       */
      foreach(lc, local_exprs) {
        Node* qual  = lfirst(lc);

        outer_plan->qual = list_delete(outer_plan->qual, qual);
        /* For an inner join the local conditions of foreign scan plan can be part of the joinquals as well.
         * (They might also be in the mergequals or hashquals, but we can't touch those without breaking the plan.)
         */
        if (IsA(outer_plan, NestLoop) || IsA(outer_plan, MergeJoin) || IsA(outer_plan, HashJoin)) {
          Join* join_plan = (Join*) outer_plan;

          if (join_plan->jointype == JOIN_INNER)
            join_plan->joinqual = list_delete(join_plan->joinqual, qual);
        }
      }
      /* Now fix the subplan's tlist --- this might result in inserting a Result node atop the plan tree. */
      outer_plan = change_plan_targetlist(outer_plan, fdw_scan_tlist, best_path->path.parallel_safe);
    }
  }

  /* Build the query string to be sent for execution, and identify expressions to be sent as parameters. */
  initStringInfo(&sql);
  deparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist, remote_exprs, best_path->path.pathkeys, has_final_sort, has_limit, false, &retrieved_attrs, &params_list);
  db2Debug2("  deparsed foreign query: %s", sql.data);
  /* Remember remote_exprs for possible use by postgresPlanDirectModify */
  fpinfo->final_remote_exprs = remote_exprs;

  // TODO serialize the whole Db2FdwState as used to including these parameters and ensure the deserialization restores them correctly
  
  /* Build the fdw_private list that will be available to the executor.
   * Items in the list must match order in enum FdwScanPrivateIndex.
   */
  fpinfo->query          = sql.data;
  fpinfo->retrieved_attr = retrieved_attrs;
  fdw_private = serializePlanData(fpinfo);

//  fdw_private = list_make3(makeString(sql.data), retrieved_attrs, makeInteger(fpinfo->fetch_size));
//  if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
//    fdw_private = lappend(fdw_private, makeString(fpinfo->relation_name));

  /* Create the ForeignScan node for the given relation.
   *
   * Note that the remote parameter expressions are stored in the fdw_exprs
   * field of the finished plan node; we can't keep them in private state
   * because then they wouldn't be subject to later planner processing.
   */
  fscan = make_foreignscan(tlist, local_exprs, scan_relid, params_list, fdw_private, fdw_scan_tlist, fdw_recheck_quals, outer_plan);
  db2Debug1("< %s::db2GetForeignPlan : %x",__FILE__,fscan);
  return fscan;
}
