#include <postgres.h>
#include <foreign/fdwapi.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>
#include <optimizer/clauses.h>
#include <optimizer/tlist.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);
extern void         db2Debug3                 (const char* message, ...);
extern void*        db2alloc                  (const char* type, size_t size);
extern char*        db2strdup                 (const char* source);
extern char*        deparseExpr               (DB2Session* session, RelOptInfo* foreignrel, Expr* expr, const DB2Table* db2Table, List** params);

/** local prototypes */
void                db2GetForeignUpperPaths   (PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra);
DB2FdwState*        db2CloneFdwStateUpper     (PlannerInfo* root, const DB2FdwState* fdw_in, RelOptInfo* input_rel, RelOptInfo* output_rel);
DB2Table*           db2CloneDb2TableForPlan   (const DB2Table* src);
DB2Column*          db2CloneDb2ColumnForPlan  (const DB2Column* src);
bool                db2_is_shippable          (PlannerInfo* root, UpperRelationKind stage, RelOptInfo* input_rel, RelOptInfo* output_rel, const DB2FdwState* fdw_in);
bool                db2_is_shippable_expr     (PlannerInfo* root, RelOptInfo* foreignrel, const DB2FdwState* fdw_in, Expr* expr, const char* label);

void db2GetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra) {
  db2Debug1("> %s::db2GetForeignUpperPaths",__FILE__);
  if (root != NULL && root->parse != NULL && output_rel->fdw_private == NULL) {
    DB2FdwState* fdw_in = NULL;
    Query*       query  = root->parse;
    db2Debug3("  query->hasAggs        : %s", query->hasAggs         ? "true" : "false");
    db2Debug3("  query->hasWindowFuncs : %s", query->hasWindowFuncs  ? "true" : "false");
    db2Debug3("  query->hasDistinctOn  : %s", query->hasDistinctOn   ? "true" : "false");
    db2Debug3("  query->hasTargetSRFs  : %s", query->hasTargetSRFs   ? "true" : "false");
    db2Debug3("  query->hasForUpdate   : %s", query->hasForUpdate    ? "true" : "false");
    db2Debug3("  query->hasGroupRTE    : %s", query->hasGroupRTE     ? "true" : "false");
    db2Debug3("  query->hasModifyingCTE: %s", query->hasModifyingCTE ? "true" : "false");
    db2Debug3("  query->hasRecursive   : %s", query->hasRecursive    ? "true" : "false");
    db2Debug3("  query->hasSubLinks    : %s", query->hasSubLinks     ? "true" : "false");
    db2Debug3("  query->hasRowSecurity : %s", query->hasRowSecurity  ? "true" : "false");
    switch (stage) {
      case UPPERREL_SETOP:              // UNION/INTERSECT/EXCEPT
        db2Debug2("  stage: %d - UPPERREL_SETOP", stage);
      break;
      case UPPERREL_PARTIAL_GROUP_AGG:  // partial grouping/aggregation
        db2Debug2("  stage: %d - UPPERREL_PARTIAL_GROUP_AGG", stage);
        db2Debug2("  query->hasAggs: %d", query->hasAggs);
        db2Debug2("  query->groupClause: %x", query->groupClause);
        if (query->hasAggs || query->groupClause != NIL) {
          fdw_in = (DB2FdwState*)input_rel->fdw_private;
        }
      break;
      case UPPERREL_GROUP_AGG: {        // grouping/aggregation
        db2Debug2("  stage: %d - UPPERREL_GROUP_AGG", stage);
        db2Debug2("  query->hasAggs: %d", query->hasAggs);
        db2Debug2("  query->groupClause: %x", query->groupClause);
        if (query->hasAggs || query->groupClause != NIL) {
          fdw_in = (DB2FdwState*)input_rel->fdw_private;
        }
      }
      break;
      case UPPERREL_WINDOW: {           // window functions
        db2Debug2("  stage: %d - UPPERREL_WINDOW", stage);
        db2Debug2("  query->hasWindowFuncs: %d", query->hasWindowFuncs);
        if (query->hasWindowFuncs) {
          fdw_in = (DB2FdwState*)input_rel->fdw_private;
        }
      }
      break;
      case UPPERREL_PARTIAL_DISTINCT: { // partial "SELECT DISTINCT"
        db2Debug2("  stage: %d - UPPERREL_PARTIAL_DISTINCT", stage);
        db2Debug2("  query->hasDistinctOn: %d", query->hasDistinctOn);
        if (query->hasDistinctOn) {
          fdw_in = (DB2FdwState*)input_rel->fdw_private;
        }
      }
      break;
      case UPPERREL_DISTINCT: {         // "SELECT DISTINCT"
        db2Debug2("  stage: %d - UPPERREL_DISTINCT", stage);
        db2Debug2("  query->hasDistinctOn: %d", query->hasDistinctOn);
        if (query->hasDistinctOn) {
          fdw_in = (DB2FdwState*)input_rel->fdw_private;
        }
      }
      break;
      case UPPERREL_ORDERED:            // ORDER BY
        db2Debug2("  stage: %d - UPPERREL_ORDERED", stage);
        db2Debug2("  query->setOperations: %x", query->setOperations);
        if (query->setOperations != NULL) {
          fdw_in = (DB2FdwState*)input_rel->fdw_private;
        }
      break;
      case UPPERREL_FINAL:              // any remaining top-level actions
        db2Debug2("  stage: %d - UPPERREL_FINAL", stage);
      break;
      default:                          // unknown stage type
        db2Debug2("  stage: %d - unknown", stage);
      break;
    }
    if (fdw_in != NULL) {
      // verify all GROUP keys, Aggrefs, HAVING are deparsable for DB2
      if (db2_is_shippable(root, stage, input_rel, output_rel, fdw_in)) {
        // create a new state for the upper rel (copy + mark as aggregate)
        Path*        path  = NULL;
        DB2FdwState* state = db2CloneFdwStateUpper(root, fdw_in, input_rel, output_rel);
//        state->is_upper   = true;
//        state->upper_kind = stage;
        // Estimate rows/cost (can be crude initially)
        output_rel->rows    = clamp_row_est(output_rel->rows);
        state->startup_cost = 10000.0;
        state->total_cost   = state->startup_cost + output_rel->rows * 10.0;
        path = (Path*) create_foreign_upper_path( root
                                                , input_rel
#if PG_VERSION_NUM >= 90600
                                                , input_rel->reltarget /* pathtarget */
#endif
                                                , output_rel->rows
#if PG_VERSION_NUM >= 180000
                                                , 0                     /* disabled nodes (PG18+) if needed */
#endif
                                                , state->startup_cost
                                                , state->total_cost
                                                , NIL
#if PG_VERSION_NUM >= 90500
                                                , NULL                  /* required_outer */
#endif
#if PG_VERSION_NUM >= 170000
                                                , NIL                   /* fdw_outerpath */
#endif
                                                , (void*)state
                                                );
//        add_path(input_rel, path);
      }
    } else {
      db2Debug2("  not pushable");
    }
  } else {
    db2Debug2("  skipping this call");
    db2Debug2("  root: %x", root);
    db2Debug2("  root->parse: %x", root->parse);
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
DB2FdwState* db2CloneFdwStateUpper(PlannerInfo* root, const DB2FdwState* fdw_in, RelOptInfo* input_rel, RelOptInfo* output_rel) {
  DB2FdwState* copy = NULL;

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
  db2Debug1("< %s::db2CloneFdwStateUpper : %x", __FILE__, copy);
  return copy;
}

DB2Table* db2CloneDb2TableForPlan(const DB2Table* src) {
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

DB2Column* db2CloneDb2ColumnForPlan(const DB2Column* src) {
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

bool db2_is_shippable(PlannerInfo* root, UpperRelationKind stage, RelOptInfo* input_rel, RelOptInfo* output_rel, const DB2FdwState* fdw_in) {
  bool fResult = false;

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
            if (!db2_is_shippable_expr(root, input_rel, fdw_in, (Expr*) tle->expr, "GROUP BY")) {
              fResult = false;
              break;
            }
          }

          if (fResult) {
            /* 2) HAVING clause must be deparsable (if present). */
            if (query->havingQual != NULL) {
              if (!db2_is_shippable_expr(root, input_rel, fdw_in, (Expr*) query->havingQual, "HAVING")) {
                fResult = false;
              }
            }
          }

          if (fResult) {
            /* 3) Output target expressions must be deparsable too. */
            foreach (lc, output_rel->reltarget->exprs) {
              Expr* expr = (Expr*) lfirst(lc);
              if (!db2_is_shippable_expr(root, input_rel, fdw_in, expr, "SELECT")) {
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

bool db2_is_shippable_expr(PlannerInfo* root, RelOptInfo* foreignrel, const DB2FdwState* fdw_in, Expr* expr, const char* label) {
  bool  fResult  = false;

  db2Debug1("> %s::db2_is_shippable_expr", __FILE__);
  if (expr == NULL) {
    fResult = true;
  } else if (fdw_in == NULL) {
    fResult = false;
  } else if (contain_agg_clause((Node*) expr)) {
    List* params   = NIL;
    char* deparsed = NULL;
    deparsed = deparseExpr(fdw_in->session, foreignrel, expr, fdw_in->db2Table, &params);
    db2Debug2("  deparsed: %s", deparsed);
    fResult = (deparsed != NULL);
  } else if (contain_window_function((Node*) expr)) {
    db2Debug2("  %s contains window function; not shippable", label ? label : "expr");
    fResult = false;
  } else {
    List* params   = NIL;
    char* deparsed = NULL;
    deparsed = deparseExpr(fdw_in->session, foreignrel, expr, fdw_in->db2Table, &params);
    db2Debug2("  deparsed: %s", deparsed);
    fResult = (deparsed != NULL);
  }
  db2Debug1("> %s::db2_is_shippable_expr : %s", __FILE__, fResult ? "true" : "false");
  return fResult;
}
