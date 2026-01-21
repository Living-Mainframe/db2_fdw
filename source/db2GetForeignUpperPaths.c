#include <postgres.h>
#include <foreign/fdwapi.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_opfamily.h>
#include <nodes/makefuncs.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>
#include <optimizer/clauses.h>
#include <optimizer/tlist.h>
#include <optimizer/restrictinfo.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"
#include "DB2FdwPathExtraData.h"

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);
extern void         db2Debug3                 (const char* message, ...);
extern void*        db2alloc                  (const char* type, size_t size);
extern char*        db2strdup                 (const char* source);
extern void*        db2free                   (void* p);
extern char*        deparseExpr               (PlannerInfo* root, RelOptInfo* foreignrel, Expr* expr, List** params);
extern bool         is_shippable              (Oid objectId, Oid classId, DB2FdwState* fpinfo);
extern bool         is_foreign_param          (PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern bool         is_foreign_expr           (PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern bool         is_foreign_pathkey        (PlannerInfo *root, RelOptInfo *baserel, PathKey *pathkey);

/** local prototypes */
void                db2GetForeignUpperPaths   (PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra);
static void         db2CloneFdwStateUpper     (PlannerInfo* root, RelOptInfo* input_rel, RelOptInfo* output_rel);
static DB2Table*    db2CloneDb2TableForPlan   (const DB2Table* src);
static DB2Column*   db2CloneDb2ColumnForPlan  (const DB2Column* src);
static bool         db2_is_shippable          (PlannerInfo* root, UpperRelationKind stage, RelOptInfo* input_rel, RelOptInfo* output_rel);
static bool         db2_is_shippable_expr     (PlannerInfo* root, RelOptInfo* foreignrel, Expr* expr, const char* label);
static void         add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *grouped_rel, GroupPathExtraData *extra);
static void         add_foreign_ordered_paths (PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *ordered_rel);
static void         add_foreign_final_paths   (PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *final_rel, FinalPathExtraData *extra);
static bool         foreign_grouping_ok       (PlannerInfo *root, RelOptInfo *grouped_rel, Node *havingQual);

void db2GetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra) {
  db2Debug1("> %s::db2GetForeignUpperPaths",__FILE__);
  if (root != NULL && root->parse != NULL && input_rel->fdw_private != NULL && output_rel->fdw_private == NULL) {
    Query*       query   = root->parse;
    db2Debug3("  query->hasAggs        : %s", query->hasAggs         ? "true" : "false");
    db2Debug3("  query->hasWindowFuncs : %s", query->hasWindowFuncs  ? "true" : "false");
    db2Debug3("  query->hasDistinctOn  : %s", query->hasDistinctOn   ? "true" : "false");
    db2Debug3("  query->hasTargetSRFs  : %s", query->hasTargetSRFs   ? "true" : "false");
    db2Debug3("  query->hasForUpdate   : %s", query->hasForUpdate    ? "true" : "false");
    #if PG_VERSION_NUM >= 180000
    db2Debug3("  query->hasGroupRTE    : %s", query->hasGroupRTE     ? "true" : "false");
    #endif
    db2Debug3("  query->hasModifyingCTE: %s", query->hasModifyingCTE ? "true" : "false");
    db2Debug3("  query->hasRecursive   : %s", query->hasRecursive    ? "true" : "false");
    db2Debug3("  query->hasSubLinks    : %s", query->hasSubLinks     ? "true" : "false");
    db2Debug3("  query->hasRowSecurity : %s", query->hasRowSecurity  ? "true" : "false");

    if (db2_is_shippable(root, stage, input_rel, output_rel)) {
      db2CloneFdwStateUpper(root, input_rel, output_rel);

      switch (stage) {
        case UPPERREL_SETOP:              // UNION/INTERSECT/EXCEPT
          db2Debug2("  stage: %d - UPPERREL_SETOP", stage);
        break;
        case UPPERREL_PARTIAL_GROUP_AGG:  // partial grouping/aggregation
          db2Debug2("  stage: %d - UPPERREL_PARTIAL_GROUP_AGG", stage);
          db2Debug2("  query->hasAggs: %d", query->hasAggs);
          db2Debug2("  query->groupClause: %x", query->groupClause);
          if (query->hasAggs || query->groupClause != NIL) {
            add_foreign_grouping_paths(root, input_rel, output_rel, (GroupPathExtraData*) extra);
          }
        break;
        case UPPERREL_GROUP_AGG: {        // grouping/aggregation
          db2Debug2("  stage: %d - UPPERREL_GROUP_AGG", stage);
          db2Debug2("  query->hasAggs: %d", query->hasAggs);
          db2Debug2("  query->groupClause: %x", query->groupClause);
          if (query->hasAggs || query->groupClause != NIL) {
            add_foreign_grouping_paths(root, input_rel, output_rel, (GroupPathExtraData*) extra);
          }
        }
        break;
        case UPPERREL_WINDOW: {           // window functions
          db2Debug2("  stage: %d - UPPERREL_WINDOW", stage);
          db2Debug2("  query->hasWindowFuncs: %d", query->hasWindowFuncs);
          if (query->hasWindowFuncs) {
            db2Debug2("  window function push down not yet implemented");
          }
        }
        break;
        #if PG_VERSION_NUM >= 150000
        case UPPERREL_PARTIAL_DISTINCT: { // partial "SELECT DISTINCT"
          db2Debug2("  stage: %d - UPPERREL_PARTIAL_DISTINCT", stage);
          db2Debug2("  query->hasDistinctOn: %d", query->hasDistinctOn);
          if (query->hasDistinctOn) {
            db2Debug2("  distinct function push down not yet implemented");
          }
        }
        break;
        #endif
        case UPPERREL_DISTINCT: {         // "SELECT DISTINCT"
          db2Debug2("  stage: %d - UPPERREL_DISTINCT", stage);
          db2Debug2("  query->hasDistinctOn: %d", query->hasDistinctOn);
          if (query->hasDistinctOn) {
            db2Debug2("  distinct function push down not yet implemented");
          }
        }
        break;
        case UPPERREL_ORDERED:            // ORDER BY
          db2Debug2("  stage: %d - UPPERREL_ORDERED", stage);
          db2Debug2("  query->setOperations: %x", query->setOperations);
          if (query->setOperations != NULL) {
            add_foreign_ordered_paths(root, input_rel, output_rel);
          }
        break;
        case UPPERREL_FINAL:              // any remaining top-level actions
          db2Debug2("  stage: %d - UPPERREL_FINAL", stage);
          add_foreign_final_paths(root, input_rel, output_rel, (FinalPathExtraData*) extra);
        break;
        default:                          // unknown stage type
          db2Debug2("  stage: %d - unknown", stage);
        break;
      }
    } else {
      db2Debug2("  stage and or functions are not shippable to DB2");
    }
  } else {
    db2Debug2("  skipping this call");
    db2Debug2("  root: %x", root);
    db2Debug2("  root->parse: %x", root->parse);
    db2Debug2("  input_rel->fdw_private: %x", input_rel->fdw_private);
    db2Debug2("  output_rel->fdw_private: %x", output_rel->fdw_private);
  }
  db2Debug1("< %s::db2GetForeignUpperPaths",__FILE__);
}

/** db2CloneFdwStateUpper
 *   Create a deep copy suitable for upper-relation planning.
 *
 *   Rationale: planning can change mutable fields like DB2Column.used and also
 *   rewrite the params list (createQuery will NULL out entries). We must avoid
 *   those mutations affecting the original baserel/joinrel planning state.
 */
static void db2CloneFdwStateUpper(PlannerInfo* root, RelOptInfo* input_rel, RelOptInfo* output_rel) {
  DB2FdwState* fdw_in = (DB2FdwState*)input_rel->fdw_private;
  DB2FdwState* copy   = NULL;

  db2Debug1("> %s::db2CloneFdwStateUpper", __FILE__);
  if (fdw_in != NULL) {
    copy = (DB2FdwState*) db2alloc("fdw_state_upper", sizeof(DB2FdwState));

    /* Connection/session fields */
    copy->dbserver   = fdw_in->dbserver   ? db2strdup(fdw_in->dbserver)   : NULL;
    copy->user       = fdw_in->user       ? db2strdup(fdw_in->user)       : NULL;
    copy->password   = fdw_in->password   ? db2strdup(fdw_in->password)   : NULL;
    copy->jwt_token  = fdw_in->jwt_token  ? db2strdup(fdw_in->jwt_token)  : NULL;
    copy->nls_lang   = fdw_in->nls_lang   ? db2strdup(fdw_in->nls_lang)   : NULL;
    /* Planner-time session handle can be shared (it is not serialized). */
    copy->session    = fdw_in->session;

    /* Planning/execution fields */
    copy->query        = fdw_in->query ? db2strdup(fdw_in->query) : NULL;
    copy->prefetch     = fdw_in->prefetch;
    copy->startup_cost = fdw_in->startup_cost;
    copy->total_cost   = fdw_in->total_cost;
    copy->rowcount     = 0;
    copy->columnindex  = 0;
    copy->temp_cxt     = NULL;

    copy->order_clause = fdw_in->order_clause ? db2strdup(fdw_in->order_clause) : NULL;
    copy->where_clause = fdw_in->where_clause ? db2strdup(fdw_in->where_clause) : NULL;

    /* Shallow-copy expression lists (Expr nodes are immutable at this stage), but
     * ensure list cells are independent because createQuery mutates the list.
     */
    copy->params       = fdw_in->params       ? list_copy(fdw_in->params)       : NIL;
    copy->remote_conds = fdw_in->remote_conds ? list_copy(fdw_in->remote_conds) : NIL;
    copy->local_conds  = fdw_in->local_conds  ? list_copy(fdw_in->local_conds)  : NIL;

    /* Deep-copy DB2 table/columns because DB2Column.used is re-derived for each
     * planned query shape.
     */
    copy->db2Table = db2CloneDb2TableForPlan(fdw_in->db2Table);

    /* Join info: keep as-is (typically NULL for baserels). */
    copy->outerrel    = fdw_in->outerrel;
    copy->innerrel    = fdw_in->innerrel;
    copy->jointype    = fdw_in->jointype;
    copy->joinclauses = fdw_in->joinclauses ? list_copy(fdw_in->joinclauses) : NIL;

    /* paramList is constructed at execution time from fdw_exprs. */
    copy->paramList = NULL;
  }
  output_rel->fdw_private = copy;
  db2Debug1("< %s::db2CloneFdwStateUpper", __FILE__);
}

static DB2Table* db2CloneDb2TableForPlan(const DB2Table* src) {
  DB2Table* dst = NULL;
  int i;

  db2Debug1("> %s::db2CloneDb2TableForPlan", __FILE__);
  if (src != NULL) {
    dst = (DB2Table*) db2alloc("db2_table_clone", sizeof(DB2Table));

    dst->name    = src->name   ? db2strdup(src->name)   : NULL;
    dst->pgname  = src->pgname ? db2strdup(src->pgname) : NULL;
    dst->batchsz = src->batchsz;
    dst->ncols   = src->ncols;
    dst->npgcols = src->npgcols;

    if (src->ncols > 0) {
      dst->cols = (DB2Column**) db2alloc("db2_table_clone->cols", sizeof(DB2Column*) * src->ncols);
      for (i = 0; i < src->ncols; ++i) {
        dst->cols[i] = db2CloneDb2ColumnForPlan(src->cols[i]);
      }
    } else {
      dst->cols = NULL;
    }
  }
  db2Debug1("< %s::db2CloneDb2TableForPlan : %x", __FILE__, dst);
  return dst;
}

static DB2Column* db2CloneDb2ColumnForPlan(const DB2Column* src) {
  DB2Column* dst = NULL;

  db2Debug1("> %s::db2CloneDb2ColumnForPlan", __FILE__);
  if (src != NULL) {
    dst = (DB2Column*) db2alloc("db2_column_clone", sizeof(DB2Column));
    /* start with a struct copy, then fix up pointer members */
    *dst = *src;

    dst->colName = src->colName ? db2strdup(src->colName) : NULL;
    dst->pgname  = src->pgname  ? db2strdup(src->pgname)  : NULL;

    /* Never share row buffers between planning states. */
    dst->val      = NULL;
    dst->val_len  = 0;
    dst->val_null = 1;
  }
  db2Debug1("< %s::db2CloneDb2ColumnForPlan : %x", __FILE__, dst);
  return dst;
}

static bool db2_is_shippable(PlannerInfo* root, UpperRelationKind stage, RelOptInfo* input_rel, RelOptInfo* output_rel) {
  bool         fResult = false;
  DB2FdwState* fdw_in  = (DB2FdwState*)input_rel->fdw_private;

  db2Debug1("> %s::db2_is_shippable", __FILE__);
  if (root == NULL || root->parse == NULL || input_rel == NULL || output_rel == NULL || fdw_in == NULL) {
    db2Debug2("  missing context; not shippable");
    fResult = false;
  } else {
    Query* query = query = root->parse;
    /* Conservatively reject query shapes we don't yet know how to translate.
     * (This can be relaxed as we add DB2 SQL support.)
     */
    if (query->hasSubLinks || query->hasWindowFuncs || query->hasDistinctOn || query->hasTargetSRFs || query->hasForUpdate || query->hasModifyingCTE || query->hasRecursive || query->hasRowSecurity) {
      db2Debug2("  query has unsupported features; not shippable");
      fResult = false;
    } else {
      switch (stage) {
        case UPPERREL_PARTIAL_GROUP_AGG:
        case UPPERREL_GROUP_AGG: {
          ListCell* lc = NULL;

          fResult = true;
          /* 1) GROUP BY expressions must be deparsable. */
          foreach (lc, query->groupClause) {
            SortGroupClause* grp = (SortGroupClause*) lfirst(lc);
            TargetEntry*     tle = get_sortgroupclause_tle(grp, query->targetList);
            if (tle == NULL || tle->expr == NULL) {
              db2Debug2("  missing GROUP BY target entry; not shippable");
              fResult = false;
              break;
            } 
            if (!db2_is_shippable_expr(root, input_rel, (Expr*) tle->expr, "GROUP BY")) {
              fResult = false;
              break;
            }
          }

          if (fResult) {
            /* 2) HAVING clause must be deparsable (if present). */
            if (query->havingQual != NULL) {
              if (!db2_is_shippable_expr(root, input_rel, (Expr*) query->havingQual, "HAVING")) {
                fResult = false;
              }
            }
          }

          if (fResult) {
            /* 3) Output target expressions must be deparsable too. */
            foreach (lc, output_rel->reltarget->exprs) {
              Expr* expr = (Expr*) lfirst(lc);
              if (!db2_is_shippable_expr(root, input_rel, expr, "SELECT")) {
                fResult = false;
                break;
              }
            }
          }
        }
        break;
        default: {
          db2Debug2("  stage %d not supported; not shippable", stage);
          fResult = false;
        }
        break;
      }
    }
  }
  db2Debug1("< %s::db2_is_shippable : %s", __FILE__, fResult ? "true" : "false");
  return fResult;
}

static bool db2_is_shippable_expr(PlannerInfo* root, RelOptInfo* foreignrel, Expr* expr, const char* label) {
  bool         fResult  = false;
  DB2FdwState* fdw_in   = (DB2FdwState*)foreignrel->fdw_private;

  db2Debug1("> %s::db2_is_shippable_expr", __FILE__);
  if (expr == NULL) {
    fResult = true;
  } else if (fdw_in == NULL) {
    fResult = false;
  } else if (contain_agg_clause((Node*) expr)) {
    List* params   = NIL;
    char* deparsed = NULL;
    deparsed = deparseExpr(root, foreignrel, expr, &params);
    db2Debug2("  deparsed: %s", deparsed);
    fResult = (deparsed != NULL);
    db2free(deparsed);
  } else if (contain_window_function((Node*) expr)) {
    db2Debug2("  %s contains window function; not shippable", label ? label : "expr");
    fResult = false;
  } else {
    List* params   = NIL;
    char* deparsed = NULL;
    deparsed = deparseExpr(root, foreignrel, expr, &params);
    db2Debug2("  deparsed: %s", deparsed);
    fResult = (deparsed != NULL);
    db2free(deparsed);
  }
  db2Debug1("> %s::db2_is_shippable_expr : %s", __FILE__, fResult ? "true" : "false");
  return fResult;
}

/** add_foreign_grouping_paths
 *  Add foreign path for grouping and/or aggregation.
 *  Given input_rel represents the underlying scan.  The paths are added to the given grouped_rel.
 */
static void add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *grouped_rel, GroupPathExtraData *extra) {
  Query*       parse   = root->parse;
  DB2FdwState* ifpinfo = (DB2FdwState*)input_rel->fdw_private;
  DB2FdwState* fpinfo  = (DB2FdwState*)grouped_rel->fdw_private;
  ForeignPath* grouppath;
  double       rows;
  int          width;
  int          disabled_nodes;
  Cost         startup_cost;
  Cost         total_cost;
  /* Nothing to be done, if there is no grouping or aggregation required. */
  if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&	!root->hasHavingQual)
    return;

  Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE || extra->patype == PARTITIONWISE_AGGREGATE_FULL);
  /* save the input_rel as outerrel in fpinfo */
  fpinfo->outerrel = input_rel;

// All of this is redundant since fpinfo is 1:1 clone of ifpinfo
  // Copy foreign table, foreign server, user mapping, FDW options etc. details from the input relation's fpinfo.
//  fpinfo->db2Table = db2CloneDb2TableForPlan(ifpinfo->db2Table);
//  fpinfo->dbserver = db2strdup(ifpinfo->dbserver);
//  fpinfo->user     = db2strdup(ifpinfo->user);
//  merge_fdw_options(fpinfo, ifpinfo, NULL);

  /** Assess if it is safe to push down aggregation and grouping.
   *
   * Use HAVING qual from extra. In case of child partition, it will have translated Vars.
   */
  if (!foreign_grouping_ok(root, grouped_rel, extra->havingQual))
    return;
  /** Compute the selectivity and cost of the local_conds, so we don't have
   * to do it over again for each path.  (Currently we create just a single
   * path here, but in future it would be possible that we build more paths
   * such as pre-sorted paths as in postgresGetForeignPaths and
   * postgresGetForeignJoinPaths.)  The best we can do for these conditions
   * is to estimate selectivity on the basis of local statistics.
   */
  fpinfo->local_conds_sel = clauselist_selectivity(root, fpinfo->local_conds, 0, JOIN_INNER, NULL);
  cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

  /* Estimate the cost of push down */
  estimate_path_cost_size(root, grouped_rel, NIL, NIL, NULL, &rows, &width, &disabled_nodes, &startup_cost, &total_cost);
  /* Now update this information in the fpinfo */
  fpinfo->rows           = rows;
  fpinfo->width          = width;
  fpinfo->disabled_nodes = disabled_nodes;
  fpinfo->startup_cost   = startup_cost;
  fpinfo->total_cost     = total_cost;
    /* Create and add foreign path to the grouping relation. */
  grouppath = create_foreign_upper_path( root
                                       , grouped_rel
                                       , grouped_rel->reltarget
                                       , rows
                                       , disabled_nodes
                                       , startup_cost
                                       , total_cost
                                       , NIL                    /* no pathkeys */
                                       , NULL
                                       , NIL                    /* no fdw_restrictinfo list */
                                       , NIL);                  /* no fdw_private */
  /* Add generated path into grouped_rel by add_path(). */
  add_path(grouped_rel, (Path*) grouppath);
}

/** add_foreign_ordered_paths
 *  Add foreign paths for performing the final sort remotely.
 * Given input_rel contains the source-data Paths.  The paths are added to the given ordered_rel.
 */
static void add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *ordered_rel) {
  Query*               parse   = root->parse;
  DB2FdwState*         ifpinfo = (DB2FdwState*)input_rel->fdw_private;
  DB2FdwState*         fpinfo  = (DB2FdwState*)ordered_rel->fdw_private;
  DB2FdwPathExtraData* fpextra;
  double               rows;
  int                  width;
  int                  disabled_nodes;
  Cost                 startup_cost;
  Cost                 total_cost;
  List*                fdw_private;
  ForeignPath*         ordered_path;
  ListCell*            lc;

  // Shouldn't get here unless the query has ORDER BY
  Assert(parse->sortClause);

  // We don't support cases where there are any SRFs in the targetlist
  if (parse->hasTargetSRFs)
    return;
  // Save the input_rel as outerrel in fpinfo
  fpinfo->outerrel = input_rel;

  // Copy foreign table, foreign server, user mapping, FDW options etc. details from the input relation's fpinfo.
  fpinfo->table = ifpinfo->table;
  fpinfo->server = ifpinfo->server;
  fpinfo->user = ifpinfo->user;
  merge_fdw_options(fpinfo, ifpinfo, NULL);

  /** If the input_rel is a base or join relation, we would already have
   * considered pushing down the final sort to the remote server when
   * creating pre-sorted foreign paths for that relation, because the
   * query_pathkeys is set to the root->sort_pathkeys in that case (see
   * standard_qp_callback()).
   */
  if (input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL) {
    Assert(root->query_pathkeys == root->sort_pathkeys);
    /* Safe to push down if the query_pathkeys is safe to push down */
    fpinfo->pushdown_safe = ifpinfo->qp_is_pushdown_safe;
    return;
  }
  // The input_rel should be a grouping relation
  Assert(input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_GROUP_AGG);
  /** We try to create a path below by extending a simple foreign path for
   * the underlying grouping relation to perform the final sort remotely,
   * which is stored into the fdw_private list of the resulting path.
   */
  // Assess if it is safe to push down the final sort
  foreach(lc, root->sort_pathkeys) {
    PathKey    *pathkey = (PathKey *) lfirst(lc);
    EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
    /** is_foreign_expr would detect volatile expressions as well, but
     * checking ec_has_volatile here saves some cycles.
     */
    if (pathkey_ec->ec_has_volatile)
      return;
    /** Can't push down the sort if pathkey's opfamily is not shippable.
     */
    if (!is_shippable(pathkey->pk_opfamily, OperatorFamilyRelationId, fpinfo))
      return;
    /** The EC must contain a shippable EM that is computed in input_rel's
     * reltarget, else we can't push down the sort.
     */
    if (find_em_for_rel_target(root, pathkey_ec, input_rel) == NULL)
      return;
  }
  /* Safe to push down */
  fpinfo->pushdown_safe = true;
  /* Construct PgFdwPathExtraData */
  fpextra = palloc0_object(DB2FdwPathExtraData);
  fpextra->target = root->upper_targets[UPPERREL_ORDERED];
  fpextra->has_final_sort = true;
  /* Estimate the costs of performing the final sort remotely */
  estimate_path_cost_size(root, input_rel, NIL, root->sort_pathkeys, fpextra, &rows, &width, &disabled_nodes,	&startup_cost, &total_cost);
  /*
   * Build the fdw_private list that will be used by postgresGetForeignPlan.
   * Items in the list must match order in enum FdwPathPrivateIndex.
   */
  fdw_private = list_make2(makeBoolean(true), makeBoolean(false));
  /* Create foreign ordering path */
  ordered_path = create_foreign_upper_path( root
                                          , input_rel
                                          , root->upper_targets[UPPERREL_ORDERED]
                                          , rows
                                          , disabled_nodes
                                          , startup_cost
                                          , total_cost
                                          , root->sort_pathkeys
                                          , NULL                   /* no extra plan */
                                          , NIL                    /* no fdw_restrictinfo list */
                                          , fdw_private);
  /* and add it to the ordered_rel */
  add_path(ordered_rel, (Path *) ordered_path);
}

/** add_foreign_final_paths
 *  Add foreign paths for performing the final processing remotely.
 *  Given input_rel contains the source-data Paths.  The paths are added to the given final_rel.
 */
static void add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *final_rel, FinalPathExtraData *extra) {
  Query*               parse                    = root->parse;
  DB2FdwState*         ifpinfo                  = (DB2FdwState *) input_rel->fdw_private;
  DB2FdwState*         fpinfo                   = (DB2FdwState *) final_rel->fdw_private;
  bool                 has_final_sort           = false;
  List*                pathkeys                 = NIL;
  DB2FdwPathExtraData* fpextra                  = NULL;
  bool                 save_use_remote_estimate = false;
  double               rows                     = 0;
  int                  width                    = 0;
  int                  disabled_nodes           = 0;
  Cost                 startup_cost             ;
  Cost                 total_cost               ;
  List*                fdw_private              = NIL;
  ForeignPath*         final_path               = NULL;

  /** Currently, we only support this for SELECT commands */
  if (parse->commandType != CMD_SELECT)
    return;

  // No work if there is no FOR UPDATE/SHARE clause and if there is no need to add a LIMIT node
  if (!parse->rowMarks && !extra->limit_needed)
    return;

  // We don't support cases where there are any SRFs in the targetlist
  if (parse->hasTargetSRFs)
    return;

  /* Save the input_rel as outerrel in fpinfo */
  fpinfo->outerrel = input_rel;

  // Copy foreign table, foreign server, user mapping, FDW options etc. details from the input relation's fpinfo.
  fpinfo->table  = ifpinfo->table;
  fpinfo->server = ifpinfo->server;
  fpinfo->user   = ifpinfo->user;
  merge_fdw_options(fpinfo, ifpinfo, NULL);

  /** If there is no need to add a LIMIT node, there might be a ForeignPath
   * in the input_rel's pathlist that implements all behavior of the query.
   * Note: we would already have accounted for the query's FOR UPDATE/SHARE
   * (if any) before we get here.
   */
  if (!extra->limit_needed) {
    ListCell   *lc;

    Assert(parse->rowMarks);

    /** Grouping and aggregation are not supported with FOR UPDATE/SHARE,
     *  so the input_rel should be a base, join, or ordered relation; and
     *  if it's an ordered relation, its input relation should be a base or
     *  join relation.
     */
    Assert(input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL  || (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_ORDERED && (ifpinfo->outerrel->reloptkind == RELOPT_BASEREL || ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

    foreach(lc, input_rel->pathlist) {
      Path* path = (Path*) lfirst(lc);

      /** apply_scanjoin_target_to_paths() uses create_projection_path()
       * to adjust each of its input paths if needed, whereas
       * create_ordered_paths() uses apply_projection_to_path() to do
       * that.  So the former might have put a ProjectionPath on top of
       * the ForeignPath; look through ProjectionPath and see if the
       * path underneath it is ForeignPath.
       */
      if (IsA(path, ForeignPath) || (IsA(path, ProjectionPath) && IsA(((ProjectionPath *) path)->subpath, ForeignPath))) {
        //Create foreign final path; this gets rid of a no-longer-needed outer plan (if any), which makes the EXPLAIN output look cleaner
        final_path = create_foreign_upper_path( root
                                              , path->parent
                                              , path->pathtarget
                                              , path->rows
                                              , path->disabled_nodes
                                              , path->startup_cost
                                              , path->total_cost
                                              , path->pathkeys
                                              , NULL                     /* no extra plan */
                                              , NIL                      /* no fdw_restrictinfo list */
                                              , NIL);                    /* no fdw_private */

        /* and add it to the final_rel */
        add_path(final_rel, (Path *) final_path);

        /* Safe to push down */
        fpinfo->pushdown_safe = true;

        return;
      }
    }

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

  Assert(extra->limit_needed);

  // If the input_rel is an ordered relation, replace the input_rel with its input relation
 if (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_ORDERED) {
    input_rel = ifpinfo->outerrel;
    ifpinfo = (DB2FdwState*) input_rel->fdw_private;
    has_final_sort = true;
    pathkeys = root->sort_pathkeys;
  }

  /* The input_rel should be a base, join, or grouping relation */
  Assert(input_rel->reloptkind == RELOPT_BASEREL || input_rel->reloptkind == RELOPT_JOINREL || (input_rel->reloptkind == RELOPT_UPPER_REL && ifpinfo->stage == UPPERREL_GROUP_AGG));

  /** We try to create a path below by extending a simple foreign path for
   * the underlying base, join, or grouping relation to perform the final
   * sort (if has_final_sort) and the LIMIT restriction remotely, which is
   * stored into the fdw_private list of the resulting path.  (We
   * re-estimate the costs of sorting the underlying relation, if
   * has_final_sort.)
   */

  /** Assess if it is safe to push down the LIMIT and OFFSET to the remote server
   */

  /** If the underlying relation has any local conditions, the LIMIT/OFFSET cannot be pushed down.
   */
  if (ifpinfo->local_conds)
    return;

  /** If the query has FETCH FIRST .. WITH TIES, 1) it must have ORDER BY as
   * well, which is used to determine which additional rows tie for the last
   * place in the result set, and 2) ORDER BY must already have been
   * determined to be safe to push down before we get here.  So in that case
   * the FETCH clause is safe to push down with ORDER BY if the remote
   * server is v13 or later, but if not, the remote query will fail entirely
   * for lack of support for it.  Since we do not currently have a way to do
   * a remote-version check (without accessing the remote server), disable
   * pushing the FETCH clause for now.
   */
  if (parse->limitOption == LIMIT_OPTION_WITH_TIES)
    return;

  /** Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
   * not safe to remote.
   */
  if (!is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset) || !is_foreign_expr(root, input_rel, (Expr *) parse->limitCount))
    return;

  /* Safe to push down */
  fpinfo->pushdown_safe = true;

  /* Construct DB2FdwPathExtraData */
  fpextra                 = palloc0_object(DB2FdwPathExtraData);
  fpextra->target         = root->upper_targets[UPPERREL_FINAL];
  fpextra->has_final_sort = has_final_sort;
  fpextra->has_limit      = extra->limit_needed;
  fpextra->limit_tuples   = extra->limit_tuples;
  fpextra->count_est      = extra->count_est;
  fpextra->offset_est     = extra->offset_est;

  /** Estimate the costs of performing the final sort and the LIMIT
   * restriction remotely.  If has_final_sort is false, we wouldn't need to
   * execute EXPLAIN anymore if use_remote_estimate, since the costs can be
   * roughly estimated using the costs we already have for the underlying
   * relation, in the same way as when use_remote_estimate is false.  Since
   * it's pretty expensive to execute EXPLAIN, force use_remote_estimate to
   * false in that case.
   */
  if (!fpextra->has_final_sort) {
    save_use_remote_estimate = ifpinfo->use_remote_estimate;
    ifpinfo->use_remote_estimate = false;
  }
  estimate_path_cost_size(root, input_rel, NIL, pathkeys, fpextra, &rows, &width, &disabled_nodes, &startup_cost, &total_cost);
  if (!fpextra->has_final_sort)
    ifpinfo->use_remote_estimate = save_use_remote_estimate;

  /** Build the fdw_private list that will be used by postgresGetForeignPlan.
   * Items in the list must match order in enum FdwPathPrivateIndex.
   */
  fdw_private = list_make2(makeBoolean(has_final_sort), makeBoolean(extra->limit_needed));

  /** Create foreign final path; this gets rid of a no-longer-needed outer
   * plan (if any), which makes the EXPLAIN output look cleaner
   */
  final_path = create_foreign_upper_path( root
                                        , input_rel
                                        , root->upper_targets[UPPERREL_FINAL]
                                        , rows
                                        , disabled_nodes
                                        , startup_cost
                                        , total_cost
                                        , pathkeys
                                        , NULL                                  /* no extra plan */
                                        , NIL                                   /* no fdw_restrictinfo list */
                                        , fdw_private);

  /* and add it to the final_rel */
  add_path(final_rel, (Path*) final_path);
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to PgFdwRelationInfo of the input relation.
 */
static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel, Node *havingQual) {
  Query*       query           = root->parse;
  DB2FdwState* fpinfo          = (DB2FdwState*) grouped_rel->fdw_private;
  PathTarget*  grouping_target = grouped_rel->reltarget;
  DB2FdwState* ofpinfo         = NULL;
  ListCell*    lc              = NULL;
  int          i               = 0;
  List*        tlist           = NIL;

  /* We currently don't support pushing Grouping Sets. */
  if (query->groupingSets)
    return false;

  /* Get the fpinfo of the underlying scan relation. */
  ofpinfo = (DB2FdwState*) fpinfo->outerrel->fdw_private;

  /** If underlying scan relation has any local conditions, those conditions
   * are required to be applied before performing aggregation.  Hence the
   * aggregate cannot be pushed down.
   */
  if (ofpinfo->local_conds)
    return false;

  /** Examine grouping expressions, as well as other expressions we'd need to
   * compute, and check whether they are safe to push down to the foreign
   * server.  All GROUP BY expressions will be part of the grouping target
   * and thus there is no need to search for them separately.  Add grouping
   * expressions into target list which will be passed to foreign server.
   *
   * A tricky fine point is that we must not put any expression into the
   * target list that is just a foreign param (that is, something that
   * deparse.c would conclude has to be sent to the foreign server).  If we
   * do, the expression will also appear in the fdw_exprs list of the plan
   * node, and setrefs.c will get confused and decide that the fdw_exprs
   * entry is actually a reference to the fdw_scan_tlist entry, resulting in
   * a broken plan.  Somewhat oddly, it's OK if the expression contains such
   * a node, as long as it's not at top level; then no match is possible.
   */
  i = 0;
  foreach(lc, grouping_target->exprs) {
    Expr*     expr = (Expr *) lfirst(lc);
    Index     sgref = get_pathtarget_sortgroupref(grouping_target, i);
    ListCell* l;

    /** Check whether this expression is part of GROUP BY clause.  Note we
     * check the whole GROUP BY clause not just processed_groupClause,
     * because we will ship all of it, cf. appendGroupByClause.
     */
    if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause)) {
      TargetEntry *tle;

      /** If any GROUP BY expression is not shippable, then we cannot
       * push down aggregation to the foreign server.
       */
      if (!is_foreign_expr(root, grouped_rel, expr))
        return false;

      /** If it would be a foreign param, we can't put it into the tlist,
       * so we have to fail.
       */
      if (is_foreign_param(root, grouped_rel, expr))
        return false;

      /** Pushable, so add to tlist.  We need to create a TLE for this
       * expression and apply the sortgroupref to it.  We cannot use
       * add_to_flat_tlist() here because that avoids making duplicate
       * entries in the tlist.  If there are duplicate entries with
       * distinct sortgrouprefs, we have to duplicate that situation in
       * the output tlist.
       */
      tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
      tle->ressortgroupref = sgref;
      tlist = lappend(tlist, tle);
    } else {
      /** Non-grouping expression we need to compute.  Can we ship it
       * as-is to the foreign server?
       */
      if (is_foreign_expr(root, grouped_rel, expr) && !is_foreign_param(root, grouped_rel, expr)) {
        /* Yes, so add to tlist as-is; OK to suppress duplicates */
        tlist = add_to_flat_tlist(tlist, list_make1(expr));
      } else 	{
        /* Not pushable as a whole; extract its Vars and aggregates */
        List* aggvars = pull_var_clause((Node*) expr, PVC_INCLUDE_AGGREGATES);

        /** If any aggregate expression is not shippable, then we
         * cannot push down aggregation to the foreign server.  (We
         * don't have to check is_foreign_param, since that certainly
         * won't return true for any such expression.)
         */
        if (!is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
          return false;

        /** Add aggregates, if any, into the targetlist.  Plain Vars
         * outside an aggregate can be ignored, because they should be
         * either same as some GROUP BY column or part of some GROUP
         * BY expression.  In either case, they are already part of
         * the targetlist and thus no need to add them again.  In fact
         * including plain Vars in the tlist when they do not match a
         * GROUP BY column would cause the foreign server to complain
         * that the shipped query is invalid.
         */
        foreach(l, aggvars) {
          Expr* aggref = (Expr *) lfirst(l);

          if (IsA(aggref, Aggref))
            tlist = add_to_flat_tlist(tlist, list_make1(aggref));
        }
      }
    }
    i++;
  }

  /** Classify the pushable and non-pushable HAVING clauses and save them in
   * remote_conds and local_conds of the grouped rel's fpinfo.
   */
  if (havingQual) {
    foreach(lc, (List *) havingQual) {
      Expr	   *expr = (Expr *) lfirst(lc);
      RestrictInfo *rinfo;

      /** Currently, the core code doesn't wrap havingQuals in
       * RestrictInfos, so we must make our own.
       */
      Assert(!IsA(expr, RestrictInfo));
      rinfo = make_restrictinfo(root, expr, true, false, false, false, root->qual_security_level, grouped_rel->relids, NULL, NULL);
      if (is_foreign_expr(root, grouped_rel, expr))
        fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
      else
        fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
    }
  }

  /** If there are any local conditions, pull Vars and aggregates from it and
   * check whether they are safe to pushdown or not.
   */
  if (fpinfo->local_conds) {
    List* aggvars = NIL;

    foreach(lc, fpinfo->local_conds) {
      RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

      aggvars = list_concat(aggvars, pull_var_clause((Node*) rinfo->clause, PVC_INCLUDE_AGGREGATES));
    }

    foreach(lc, aggvars) {
      Expr	   *expr = (Expr *) lfirst(lc);

      /** If aggregates within local conditions are not safe to push
       * down, then we cannot push down the query.  Vars are already
       * part of GROUP BY clause which are checked above, so no need to
       * access them again here.  Again, we need not check
       * is_foreign_param for a foreign aggregate.
       */
      if (IsA(expr, Aggref)) {
        if (!is_foreign_expr(root, grouped_rel, expr))
          return false;

        tlist = add_to_flat_tlist(tlist, list_make1(expr));
      }
    }
  }

  /* Store generated targetlist */
  fpinfo->grouped_tlist = tlist;

  /* Safe to pushdown */
  fpinfo->pushdown_safe = true;

  /** Set # of retrieved rows and cached relation costs to some negative
   * value, so that we can detect when they are set to some sensible values,
   * during one (usually the first) of the calls to estimate_path_cost_size.
   */
  fpinfo->retrieved_rows = -1;
  fpinfo->rel_startup_cost = -1;
  fpinfo->rel_total_cost = -1;

  /** Set the string describing this grouped relation to be used in EXPLAIN
   * output of corresponding ForeignScan.  Note that the decoration we add
   * to the base relation name mustn't include any digits, or it'll confuse
   * postgresExplainForeignScan.
   */
  fpinfo->relation_name = psprintf("Aggregate on (%s)", ofpinfo->relation_name);
  return true;
}
