#include <postgres.h>
#include <foreign/fdwapi.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>
#include <optimizer/clauses.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);
extern void*        db2alloc                  (const char* type, size_t size);
extern char*        db2strdup                 (const char* source);

/** local prototypes */
void                db2GetForeignUpperPaths   (PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra);
DB2FdwState*        db2CloneFdwStateUpper     (PlannerInfo* root, const DB2FdwState* fdw_in, RelOptInfo* input_rel, RelOptInfo* output_rel);
DB2Table*           db2CloneDb2TableForPlan   (const DB2Table* src);
DB2Column*          db2CloneDb2ColumnForPlan  (const DB2Column* src);

void db2GetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra) {
  DB2FdwState* fdw_in = NULL;

  db2Debug1("> %s::db2GetForeignUpperPaths",__FILE__);
    switch (stage) {
      case UPPERREL_SETOP:              // UNION/INTERSECT/EXCEPT
        db2Debug2("  query contains UNION/INTERSECT/EXCEPT");
      break;
      case UPPERREL_PARTIAL_GROUP_AGG: {  // partial grouping/aggregation
        db2Debug2("  query contains partial grouping/aggregation");
        fdw_in = (DB2FdwState*)input_rel->fdw_private;
      }
      break;
      case UPPERREL_GROUP_AGG: {        // grouping/aggregation
        db2Debug2("  query contains grouping/aggregation");
        fdw_in = (DB2FdwState*)input_rel->fdw_private;
      }
      break;
      case UPPERREL_WINDOW:             // window functions
        db2Debug2("  query contains window functions");
      break;
      case UPPERREL_PARTIAL_DISTINCT:   // partial "SELECT DISTINCT"
        db2Debug2("  query contains partial distinct");
      break;
      case UPPERREL_DISTINCT:           // "SELECT DISTINCT"
        db2Debug2("  query contains distinct");
      break;
      case UPPERREL_ORDERED:            // ORDER BY
        db2Debug2("  query contains order by");
      break;
      case UPPERREL_FINAL:              // any remaining top-level actions
        db2Debug2("  query contains other top-level actions");
      break;
      default:                          // unknown stage type
        db2Debug2("  unknown stage type");
      break;
    }
    if (fdw_in != NULL) {
      // 1) Reject unsupported cases quickly
      if (db2_groupagg_supported(root, output_rel)) {
        // 2) Verify all GROUP keys, Aggrefs, HAVING are deparsable for DB2
        if (db2_groupagg_shippable(root, input_rel, output_rel, fdw_in)) {
          // 3) Create a new state for the upper rel (copy + mark as aggregate) 
          DB2FdwState* state = db2CloneFdwStateUpper(root, fdw_in, input_rel, output_rel);
//          state->is_upper = true;
//          state->upper_kind = UPPERREL_GROUP_AGG;
          // Estimate rows/cost (can be crude initially)
          output_rel->rows = clamp_row_est(output_rel->rows);

          Path* path = (Path*) create_foreign_upper_path( root
                                                        , output_rel
                                                        , output_rel->reltarget /* pathtarget */
                                                        , output_rel->rows
                                                        , 0                     /* disabled nodes (PG18+) if needed */
                                                        , state->startup_cost
                                                        , state->startup_cost + output_rel->rows * 10.0
                                                        , NIL                   /* no pathkeys initially */
                                                        , NULL                  /* required_outer */
                                                        , NULL                  /* fdw_outerpath */
                                                        , (void*)state
                                                        );

          add_path(output_rel, path);
        }
      }
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
  return copy;
}

DB2Table* db2CloneDb2TableForPlan(const DB2Table* src) {
  DB2Table* dst = NULL;
  int i;

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
  return dst;
}

DB2Column* db2CloneDb2ColumnForPlan(const DB2Column* src) {
  DB2Column* dst = NULL;

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
  return dst;
}
