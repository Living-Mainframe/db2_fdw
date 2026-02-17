#include <postgres.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);
extern char*        deparseExpr               (DB2Session* session, RelOptInfo * foreignrel, Expr* expr, const DB2Table* db2Table, List** params);
extern char*        db2strdup                 (const char* source);
extern void*        db2alloc                  (const char* type, size_t size);

/** local prototypes */
void db2GetForeignJoinPaths(PlannerInfo* root, RelOptInfo* joinrel, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinType jointype, JoinPathExtraData* extra);
bool foreign_join_ok       (PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, RelOptInfo* outerrel, RelOptInfo* innerrel, JoinPathExtraData* extra);

/** db2GetForeignJoinPaths
 *   Add possible ForeignPath to joinrel if the join is safe to push down.
 *   For now, we can only push down 2-way inner join for SELECT.
 */
void db2GetForeignJoinPaths (PlannerInfo * root, RelOptInfo * joinrel, RelOptInfo * outerrel, RelOptInfo * innerrel, JoinType jointype, JoinPathExtraData * extra) {
  DB2FdwState* fdwState                = NULL;
  ForeignPath* joinpath                = NULL;
  double       joinclauses_selectivity = 0;
  double       rows                    = 0;      /* estimated number of returned rows */
  Cost         startup_cost;
  Cost         total_cost;

  db2Debug1("> db2GetForeignJoinPaths");
  /*
   * Currently we don't push-down joins in query for UPDATE/DELETE.
   * This would require a path for EvalPlanQual.
   * This restriction might be relaxed in a later release.
   */
  if (root->parse->commandType != CMD_SELECT) {
    elog (DEBUG2, "db2_fdw: don't push down join because it is no SELECT");
    return;
  }

  /*
   * N-way join is not supported, due to the column definition infrastracture.
   * If we can track relid mapping of join relations, we can support N-way join.
   */
  if (!IS_SIMPLE_REL (outerrel) || !IS_SIMPLE_REL (innerrel))
    return;

  /* skip if this join combination has been considered already */
  if (joinrel->fdw_private)
    return;

  /*
   * Create unfinished DB2FdwState which is used to indicate
   * that the join relation has already been considered, so that we won't waste
   * time considering it again and don't add the same path a second time.
   * Once we know that this join can be pushed down, we fill the data structure.
   */
  fdwState = (DB2FdwState *) db2alloc("joinrel->fdw_private", sizeof (DB2FdwState));

  joinrel->fdw_private = fdwState;

  /* this performs further checks and completes joinrel->fdw_private */
  if (!foreign_join_ok (root, joinrel, jointype, outerrel, innerrel, extra))
    return;

  /* estimate the number of result rows for the join */
#if PG_VERSION_NUM < 140000
  if (outerrel->pages > 0 && innerrel->pages > 0)
#else
  if (outerrel->tuples >= 0 && innerrel->tuples >= 0)
#endif  /* PG_VERSION_NUM */
  {
    /* both relations have been ANALYZEd, so there should be useful statistics */
    joinclauses_selectivity = clauselist_selectivity(root, fdwState->joinclauses, 0, JOIN_INNER, extra->sjinfo);
    rows = clamp_row_est (innerrel->tuples * outerrel->tuples * joinclauses_selectivity);
  } else {
    /* at least one table lacks statistics, so use a fixed estimate */
    rows = 1000.0;
  }

  /* use a random "high" value for startup cost */
  startup_cost = 10000.0;

  /* estimate total cost as startup cost + (returned rows) * 10.0 */
  total_cost   = startup_cost + rows * 10.0;

  /* store cost estimation results */
  joinrel->rows          = rows;
  fdwState->startup_cost = startup_cost;
  fdwState->total_cost   = total_cost;

  /* create a new join path */
  joinpath = create_foreign_join_path( root
                                     , joinrel
                                     , NULL  /* default pathtarget */
                                     , rows
#if PG_VERSION_NUM >= 180000
                                     , 0     /* no disabled plan nodes */
#endif  /* PG_VERSION_NUM */
                                     , startup_cost
                                     , total_cost
                                     , NIL   /* no pathkeys */
                                     , joinrel->lateral_relids
                                     , NULL  /* no epq_path */
#if PG_VERSION_NUM >= 170000
                                     , NIL   /* no fdw_restrictinfo */
#endif  /* PG_VERSION_NUM */
                                     , NIL   /* no fdw_private */
                                    );
  /* add generated path to joinrel */
  add_path(joinrel, (Path *) joinpath);
  db2Debug1("< db2GetForeignJoinPaths");
}

/** foreign_join_ok
 *   Assess whether the join between inner and outer relations can be pushed down
 *   to the foreign server. As a side effect, save information we obtain in this
 *   function to DB2FdwState passed in.
 */
bool foreign_join_ok (PlannerInfo * root, RelOptInfo * joinrel, JoinType jointype, RelOptInfo * outerrel, RelOptInfo * innerrel, JoinPathExtraData * extra) {
  DB2FdwState* fdwState     = NULL;
  DB2FdwState* fdwState_o   = NULL;
  DB2FdwState* fdwState_i   = NULL;
  DB2Table*    db2Table_o   = NULL;
  DB2Table*    db2Table_i   = NULL;
  ListCell*    lc           = NULL;
  List*        otherclauses = NULL;
  char*        tabname      = NULL;/* for warning messages */

  db2Debug1("> foreign_join_ok");
  /* we only support pushing down INNER joins */
  if (jointype != JOIN_INNER)
    return false;

  fdwState   = (DB2FdwState*) joinrel->fdw_private;
  fdwState_o = (DB2FdwState*) outerrel->fdw_private;
  fdwState_i = (DB2FdwState*) innerrel->fdw_private;
  Assert (fdwState && fdwState_o && fdwState_i);

  fdwState->outerrel = outerrel;
  fdwState->innerrel = innerrel;
  fdwState->jointype = jointype;

  /*
   * If joining relations have local conditions, those conditions are
   * required to be applied before joining the relations. Hence the join can
   * not be pushed down.
   */
  if (fdwState_o->local_conds || fdwState_i->local_conds)
    return false;

  /* Separate restrict list into join quals and quals on join relation */

  /*
   * Unlike an outer join, for inner join, the join result contains only
   * the rows which satisfy join clauses, similar to the other clause.
   * Hence all clauses can be treated the same.
   */
  otherclauses = extract_actual_clauses (extra->restrictlist, false);

  /*
   * For inner joins, "otherclauses" contains now the join conditions.
   * Check which ones can be pushed down.
   */
  foreach (lc, otherclauses) {
    char *tmp = NULL;
    Expr *expr = (Expr *) lfirst (lc);

    tmp = deparseExpr (fdwState->session, joinrel, expr, fdwState->db2Table, &(fdwState->params));

    if (tmp == NULL)
      fdwState->local_conds = lappend (fdwState->local_conds, expr);
    else
      fdwState->remote_conds = lappend (fdwState->remote_conds, expr);
  }

  /*
   * Only push down joins for which all join conditions can be pushed down.
   *
   * For an inner join it would be ok to only push own some of the join
   * conditions and evaluate the others locally, but we cannot be certain
   * that such a plan is a good or even a feasible one:
   * With one of the join conditions missing in the pushed down query,
   * it could be that the "intermediate" join result fetched from the DB2
   * side has many more rows than the complete join result.
   *
   * We could rely on estimates to see how many rows are returned from such
   * a join where not all join conditions can be pushed down, but we choose
   * the safe road of not pushing down such joins at all.
   */
  if (fdwState->local_conds != NIL)
    return false;

  /* CROSS JOIN (T1 JOIN T2 ON true) is not pushed down */
  if (fdwState->remote_conds == NIL)
    return false;

  /*
   * Pull the other remote conditions from the joining relations into join
   * clauses or other remote clauses (remote_conds) of this relation
   * wherever possible. This avoids building subqueries at every join step,
   * which is not currently supported by the deparser logic.
   *
   * For an inner join, clauses from both the relations are added to the
   * other remote clauses.
   *
   * The joining sides can not have local conditions, thus no need to test
   * shippability of the clauses being pulled up.
   */
  fdwState->remote_conds = list_concat (fdwState->remote_conds, list_copy (fdwState_i->remote_conds));
  fdwState->remote_conds = list_concat (fdwState->remote_conds, list_copy (fdwState_o->remote_conds));

  /*
   * For an inner join, all restrictions can be treated alike. Treating the
   * pushed down conditions as join conditions allows a top level full outer
   * join to be deparsed without requiring subqueries.
   */
  fdwState->joinclauses = fdwState->remote_conds;
  fdwState->remote_conds = NIL;

  /* set fetch size to minimum of the joining sides */
  if (fdwState_o->prefetch < fdwState_i->prefetch)
    fdwState->prefetch = fdwState_o->prefetch;
  else
    fdwState->prefetch = fdwState_i->prefetch;

  /* copy outerrel's infomation to fdwstate */
  fdwState->dbserver = fdwState_o->dbserver;
  fdwState->user     = fdwState_o->user;
  fdwState->password = fdwState_o->password;
  fdwState->nls_lang = fdwState_o->nls_lang;

  /* construct db2Table for the result of join */
  db2Table_o = fdwState_o->db2Table;
  db2Table_i = fdwState_i->db2Table;

  fdwState->db2Table          = (DB2Table*) db2alloc("fdw_state->db2Table", sizeof (DB2Table));
  fdwState->db2Table->name    = db2strdup ("");
  fdwState->db2Table->pgname  = db2strdup ("");
  fdwState->db2Table->ncols   = 0;
  fdwState->db2Table->npgcols = 0;
  fdwState->db2Table->cols    = (DB2Column **) db2alloc("fdw_state->db2Table->cols[]", (sizeof (DB2Column*) * (db2Table_o->ncols + db2Table_i->ncols)));

  /*
   * Search db2Column from children's db2Table.
   * Here we assume that children are foreign table, not foreign join.
   * We need capability to track relid chain through join tree to support N-way join.
   */
  tabname = "?";
  foreach (lc, joinrel->reltarget->exprs) {
    int i;
    Var *var = (Var *) lfirst (lc);
    struct db2Column *col = NULL;
    struct db2Column *newcol;
    int used_flag = 0;

    Assert (IsA (var, Var));
    /* Find appropriate entry from children's db2Table. */
    for (i = 0; i < db2Table_o->ncols; ++i) {
      struct db2Column *tmp = db2Table_o->cols[i];

      if (tmp->varno == var->varno) {
        tabname = db2Table_o->pgname;

        if (tmp->pgattnum == var->varattno) {
          col = tmp;
          break;
        }
      }
    }
    if (!col) {
      for (i = 0; i < db2Table_i->ncols; ++i) {
        struct db2Column *tmp = db2Table_i->cols[i];

        if (tmp->varno == var->varno) {
          tabname = db2Table_i->pgname;

          if (tmp->pgattnum == var->varattno) {
            col = tmp;
            break;
          }
        }
      }
    }

    newcol = (DB2Column*) db2alloc("fdw_state->db2Table->cols[idx]", sizeof (DB2Column));
    if (col) {
      memcpy (newcol, col, sizeof (struct db2Column));
      used_flag = 1;
    } else {
        /* non-existing column, print a warning */
        ereport (WARNING
                ,(errcode(ERRCODE_WARNING)
                 ,errmsg ("column number %d of foreign table \"%s\" does not exist in foreign DB2 table, will be replaced by NULL"
                         ,var->varattno
                         ,tabname
                         )
                 )
                );
      }
    newcol->used = used_flag;
    /* pgattnum should be the index in SELECT clause of join query. */
    newcol->pgattnum = fdwState->db2Table->ncols + 1;

    fdwState->db2Table->cols[fdwState->db2Table->ncols++] = newcol;
  }

  fdwState->db2Table->npgcols = fdwState->db2Table->ncols;

  db2Debug1("< foreign_join_ok");
  return true;
}

