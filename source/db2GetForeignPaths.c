#include <postgres.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/pathnode.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);
extern char*        deparseExpr               (DB2Session* session, RelOptInfo * foreignrel, Expr* expr, const DB2Table* db2Table, List** params);

/** local prototypes */
void  db2GetForeignPaths  (PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
Expr* find_em_expr_for_rel(EquivalenceClass * ec, RelOptInfo * rel);

/** db2GetForeignPaths
 *   Create a ForeignPath node and add it as only possible path.
 */
void db2GetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid) {
  DB2FdwState* fdwState = (DB2FdwState*) baserel->fdw_private;

  /*
   * Determine whether we can potentially push query pathkeys to the remote
   * side, avoiding a local sort.
   */
  StringInfoData orderedquery;
  List*          usable_pathkeys = NIL;
  ListCell*      cell;
  char*          delim = " ";

  db2Debug1("> db2GetForeignPaths");
  initStringInfo (&orderedquery);

  foreach (cell, root->query_pathkeys) {
    PathKey*           pathkey    = (PathKey *) lfirst (cell);
    EquivalenceClass*  pathkey_ec = pathkey->pk_eclass;
    Expr*              em_expr    = NULL;
    char*              sort_clause;
    Oid                em_type;
    bool               can_pushdown;

    /** deparseExpr would detect volatile expressions as well, but
     * ec_has_volatile saves some cycles.
     */
    can_pushdown = !pathkey_ec->ec_has_volatile && ((em_expr = find_em_expr_for_rel (pathkey_ec, baserel)) != NULL);

    if (can_pushdown) {
      em_type = exprType ((Node *) em_expr);

      /* expressions of a type different from this are not safe to push down into ORDER BY clauses */
      if (em_type != INT8OID   && em_type != INT2OID    && em_type  != INT4OID    && em_type != OIDOID       &&  em_type != FLOAT4OID
      &&  em_type != FLOAT8OID && em_type != NUMERICOID && em_type  != DATEOID    && em_type != TIMESTAMPOID && em_type  != TIMESTAMPTZOID
      &&  em_type != TIMEOID   && em_type != TIMETZOID  &&  em_type != INTERVALOID)
        can_pushdown = false;
    }

    if (can_pushdown && ((sort_clause = deparseExpr (fdwState->session, baserel, em_expr, fdwState->db2Table, &(fdwState->params))) != NULL)) {
      /* keep usable_pathkeys for later use. */
      usable_pathkeys = lappend (usable_pathkeys, pathkey);

      /* create orderedquery */
      appendStringInfoString (&orderedquery, delim);
      appendStringInfoString (&orderedquery, sort_clause);
      delim = ", ";

      #if PG_VERSION_NUM >= 180000
      appendStringInfoString (&orderedquery, (pathkey->pk_cmptype == COMPARE_LT) ? " ASC" : " DESC");
      #else
      appendStringInfoString (&orderedquery, (pathkey->pk_strategy == BTLessStrategyNumber) ? " ASC" : " DESC");
      #endif
      appendStringInfoString (&orderedquery, (pathkey->pk_nulls_first) ? " NULLS FIRST" : " NULLS LAST");
    } else {
      /*
       * The planner and executor don't have any clever strategy for
       * taking data sorted by a prefix of the query's pathkeys and
       * getting it to be sorted by all of those pathekeys.  We'll just
       * end up resorting the entire data set.  So, unless we can push
       * down all of the query pathkeys, forget it.
       */
      list_free (usable_pathkeys);
      usable_pathkeys = NIL;
      break;
    }
  }

  /* set order clause */
  if (usable_pathkeys != NIL)
    fdwState->order_clause = orderedquery.data;

  /* add the only path */
  add_path (baserel, (Path *) create_foreignscan_path (root
                                                      ,baserel
                                                      ,NULL  /* default pathtarget */
                                                      ,baserel->rows
  #if PG_VERSION_NUM >= 180000
                                                      ,0  /* no disabled plan nodes */
  #endif  /* PG_VERSION_NUM */
                                                      ,fdwState->startup_cost
                                                      ,fdwState->total_cost
                                                      ,usable_pathkeys
                                                      ,baserel->lateral_relids
                                                      ,NULL  /* no extra plan */
  #if PG_VERSION_NUM >= 170000
                                                      ,NIL   /* no fdw_restrictinfo */
  #endif  /* PG_VERSION_NUM */
                                                      ,NIL
                                                      )
    );
  db2Debug1("< db2GetForeignPaths");
}

/* find_em_expr_for_rel
 * Find an equivalence class member expression, all of whose Vars come from
 * the indicated relation.
 */
Expr* find_em_expr_for_rel (EquivalenceClass* ec, RelOptInfo* rel) {
  ListCell* lc_em = NULL;
  Expr*     result = NULL;
  db2Debug1("> find_em_expr_for_rel");
  foreach (lc_em, ec->ec_members) {
    EquivalenceMember* em = lfirst (lc_em);
    if (bms_equal (em->em_relids, rel->relids)) {
      /*
       * If there is more than one equivalence member whose Vars are
       * taken entirely from this relation, we'll be content to choose
       * any one of those.
       */
      result =  em->em_expr;
      break;
    }
  }
  db2Debug1("< find_em_expr_for_rel - returns: %x", result);
  return result;
}
