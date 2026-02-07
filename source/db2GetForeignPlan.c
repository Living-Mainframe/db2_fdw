#include <postgres.h>
#include <optimizer/planmain.h>
#include <optimizer/restrictinfo.h>
#include <utils/lsyscache.h>
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
extern void*  db2alloc                (const char* type, size_t size);
extern char*  db2strdup               (const char* source);


extern bool   is_foreign_expr         (PlannerInfo* root, RelOptInfo* baserel, Expr* expr);
extern void   deparseSelectStmtForRel (StringInfo buf, PlannerInfo* root, RelOptInfo* rel, List* tlist, List* remote_conds, List* pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List** retrieved_attrs, List** params_list);
extern List*  build_tlist_to_deparse  (RelOptInfo* foreignrel);
extern List*  serializePlanData       (DB2FdwState* fdw_state);

/** local prototypes */
       ForeignScan* db2GetForeignPlan       (PlannerInfo* root, RelOptInfo* foreignrel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses , Plan* outer_plan);
static void         getUsedColumns          (Expr* expr, RelOptInfo* foreignrel);
static void         addResult               (DB2ResultColumn**  resultList, DB2Column* db2Column);

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
    ListCell* cell = NULL;

    /* For base relations, set scan_relid as the relid of the relation. */
    scan_relid = foreignrel->relid;

    /* find all the columns to include in the select list */
    /* examine each SELECT list entry for Var nodes */
    db2Debug3("  size of columnlist: %d", list_length(foreignrel->reltarget->exprs));
    foreach (cell, foreignrel->reltarget->exprs) {
      db2Debug3("  examine column");
      getUsedColumns ((Expr*) lfirst (cell), foreignrel);
    }

    /* examine each condition for Var nodes */
    db2Debug3("  size of conditions: %d", list_length(foreignrel->baserestrictinfo));
    foreach (cell, foreignrel->baserestrictinfo) {
      db2Debug3("  examine condition");
      getUsedColumns ((Expr*) lfirst (cell), foreignrel);
    }

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
    if (list_length(fdw_scan_tlist) > 0) {
      ListCell*   cell    = NULL;

      /* examine each condition for Tlist nodes */
      db2Debug3("  size of tlist: %d", list_length(fdw_scan_tlist));
      foreach (cell, fdw_scan_tlist) {
        db2Debug3("  examine tlist");
        getUsedColumns ((Expr*) lfirst (cell), foreignrel);
      }
    }

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

  /* Build the fdw_private list that will be available to the executor.
   * Items in the list must match order in enum FdwScanPrivateIndex.
   */
  fpinfo->db2Table->rncols  = list_length(fdw_scan_tlist);
  fpinfo->query             = sql.data;
  fpinfo->retrieved_attr    = retrieved_attrs;
  fdw_private               = serializePlanData(fpinfo);
  
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

/** getUsedColumns
 *   Set "used=true" in db2Table for all columns used in the expression.
 */
static void getUsedColumns (Expr* expr, RelOptInfo* foreignrel) {
  ListCell* cell  = NULL;

  db2Debug1("> getUsedColumns");
  if (expr != NULL) {
    switch (expr->type) {
      case T_RestrictInfo:
        getUsedColumns (((RestrictInfo*) expr)->clause, foreignrel);
      break;
      case T_TargetEntry:
        getUsedColumns (((TargetEntry*) expr)->expr, foreignrel);
      break;
      case T_Const:
      case T_Param:
      case T_CaseTestExpr:
      case T_CoerceToDomainValue:
      case T_CurrentOfExpr:
      case T_NextValueExpr:
      break;
      case T_Var: {
        DB2FdwState*  fpinfo    = (DB2FdwState*) foreignrel->fdw_private;
        Var*          variable  = NULL;
        int           index     = 0;

        variable = (Var*) expr;
        /* ignore system columns */
        if (variable->varattno < 0)
          break;
        /* if this is a wholerow reference, we need all columns */
        if (variable->varattno == 0) {
          for (index = 0; index < fpinfo->db2Table->ncols; ++index) {
            if (fpinfo->db2Table->cols[index]->pgname) {
              addResult(&fpinfo->resultList,fpinfo->db2Table->cols[index]);
//              fpinfo->db2Table->cols[index]->used = 1;
            }
          }
          break;
        }
        /* get db2Table column index corresponding to this column (-1 if none) */
        index = fpinfo->db2Table->ncols - 1;
        db2Debug2("  variable->varattno: %d", variable->varattno); 
        while (index >= 0 && fpinfo->db2Table->cols[index]->pgattnum != variable->varattno) {
          --index;
        }
        if (index == -1) {
          ereport (WARNING, (errcode (ERRCODE_WARNING),errmsg ("column number %d of foreign table \"%s\" does not exist in foreign DB2 table, will be replaced by NULL", variable->varattno, fpinfo->db2Table->pgname)));
        } else {
          addResult(&fpinfo->resultList,fpinfo->db2Table->cols[index]);
//          fpinfo->db2Table->cols[index]->used = 1;
        }
      }
      break;
      case T_Aggref: {
        Aggref* aggref = (Aggref*) expr;
        foreach (cell, aggref->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
        foreach (cell, aggref->aggorder) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
        foreach (cell, aggref->aggdistinct) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      }
      break;
      case T_WindowFunc:
        foreach (cell, ((WindowFunc*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_SubscriptingRef: {
        SubscriptingRef* ref = (SubscriptingRef*) expr;
        foreach(cell, ref->refupperindexpr) {
          getUsedColumns((Expr*)lfirst(cell), foreignrel);
        }
        foreach(cell, ref->reflowerindexpr) {
          getUsedColumns((Expr*)lfirst(cell), foreignrel);
        }
        getUsedColumns(ref->refexpr, foreignrel);
        getUsedColumns(ref->refassgnexpr, foreignrel);
      }
      break;
      case T_FuncExpr:
        foreach (cell, ((FuncExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_OpExpr:
        foreach (cell, ((OpExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_DistinctExpr:
        foreach (cell, ((DistinctExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_NullIfExpr:
        foreach (cell, ((NullIfExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_ScalarArrayOpExpr:
        foreach (cell, ((ScalarArrayOpExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_BoolExpr:
        foreach (cell, ((BoolExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_SubPlan:
        foreach (cell, ((SubPlan*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_AlternativeSubPlan:
        /* examine only first alternative */
        getUsedColumns ((Expr*) linitial (((AlternativeSubPlan*) expr)->subplans), foreignrel);
      break;
      case T_NamedArgExpr:
        getUsedColumns (((NamedArgExpr*) expr)->arg, foreignrel);
      break;
      case T_FieldSelect:
        getUsedColumns (((FieldSelect*) expr)->arg, foreignrel);
      break;
      case T_RelabelType:
        getUsedColumns (((RelabelType*) expr)->arg, foreignrel);
      break;
      case T_CoerceViaIO:
        getUsedColumns (((CoerceViaIO*) expr)->arg, foreignrel);
      break;
      case T_ArrayCoerceExpr:
        getUsedColumns (((ArrayCoerceExpr*) expr)->arg, foreignrel);
      break;
      case T_ConvertRowtypeExpr:
        getUsedColumns (((ConvertRowtypeExpr*) expr)->arg, foreignrel);
      break;
      case T_CollateExpr:
        getUsedColumns (((CollateExpr*) expr)->arg, foreignrel);
      break;
      case T_CaseExpr:
        foreach (cell, ((CaseExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
        getUsedColumns (((CaseExpr*) expr)->arg, foreignrel);
        getUsedColumns (((CaseExpr*) expr)->defresult, foreignrel);
      break;
      case T_CaseWhen:
        getUsedColumns (((CaseWhen*) expr)->expr, foreignrel);
        getUsedColumns (((CaseWhen*) expr)->result, foreignrel);
      break;
      case T_ArrayExpr:
        foreach (cell, ((ArrayExpr*) expr)->elements) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_RowExpr:
        foreach (cell, ((RowExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_RowCompareExpr:
        foreach (cell, ((RowCompareExpr*) expr)->largs) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
        foreach (cell, ((RowCompareExpr*) expr)->rargs) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_CoalesceExpr:
        foreach (cell, ((CoalesceExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_MinMaxExpr:
        foreach (cell, ((MinMaxExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_XmlExpr:
        foreach (cell, ((XmlExpr*) expr)->named_args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
        foreach (cell, ((XmlExpr*) expr)->args) {
          getUsedColumns ((Expr*) lfirst (cell), foreignrel);
        }
      break;
      case T_NullTest:
        getUsedColumns (((NullTest*) expr)->arg, foreignrel);
      break;
      case T_BooleanTest:
        getUsedColumns (((BooleanTest*) expr)->arg, foreignrel);
      break;
      case T_CoerceToDomain:
        getUsedColumns (((CoerceToDomain*) expr)->arg, foreignrel);
      break;
      case T_PlaceHolderVar:
        getUsedColumns (((PlaceHolderVar*) expr)->phexpr, foreignrel);
      break;
      case T_SQLValueFunction:
        //nop
      break;                                /* contains no column references */
      default:
        /*
         * We must be able to handle all node types that can
         * appear because we cannot omit a column from the remote
         * query that will be needed.
         * Throw an error if we encounter an unexpected node type.
         */
        ereport (ERROR, (errcode (ERRCODE_FDW_UNABLE_TO_CREATE_REPLY), errmsg ("Internal db2_fdw error: encountered unknown node type %d.", expr->type)));
       break;
    }
  }
  db2Debug1("< getUsedColumns");
}

static void addResult(DB2ResultColumn** resultList, DB2Column* column) {
  DB2ResultColumn* resCol = NULL;
  db2Debug1("> %s::addResult",__FILE__);
  resCol = db2alloc("resultColumn",sizeof(DB2ResultColumn));
  resCol->next           = *resultList;
  resCol->colName        = db2strdup(column->colName);
  resCol->colType        = column->colType;
  resCol->colSize        = column->colSize;
  resCol->colScale       = column->colScale;
  resCol->colNulls       = column->colNulls;
  resCol->colChars       = column->colChars;
  resCol->colBytes       = column->colBytes;
  resCol->colPrimKeyPart = column->colPrimKeyPart;
  resCol->colCodepage    = column->colCodepage;
  resCol->pgname         = db2strdup(column->pgname);
  resCol->pgattnum       = column->pgattnum;
  resCol->pgtype         = column->pgtype;
  resCol->pgtypmod       = column->pgtypmod;
  resCol->pkey           = column->pkey;
  resCol->val            = db2strdup(column->val);
  resCol->val_size       = column->val_size;
  resCol->val_len        = column->val_len;
  resCol->val_null       = column->val_null;
  resCol->varno          = column->varno;
  resCol->noencerr       = column->noencerr;
  *resultList            = resCol;
  db2Debug1("< %s::addResult",__FILE__);
}