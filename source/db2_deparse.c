#include <postgres.h>

#include <access/htup_details.h>
#include <access/sysattr.h>
#include <access/table.h>


#include <catalog/pg_aggregate.h>
#include <catalog/pg_authid.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_database.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_opfamily.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_ts_config.h>
#include <catalog/pg_ts_dict.h>
#include <catalog/pg_type.h>
#include <catalog/pg_collation.h>

#include <nodes/pg_list.h>
#include <nodes/pathnodes.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <nodes/bitmapset.h>

#include <optimizer/optimizer.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>

#include <parser/parsetree.h>

#include <utils/date.h>
#include <utils/datetime.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/syscache.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/typcache.h>

#include "db2_fdw.h"
#include "DB2FdwState.h"

/** Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt {
  PlannerInfo* root;        /* global planner state */
  RelOptInfo*  foreignrel;  /* the foreign relation we are planning for */
  Relids       relids;      /* relids of base relations in the underlying scan */
} foreign_glob_cxt;

/** Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum {
  FDW_COLLATE_NONE,   /* expression is of a noncollatable type, or it has default collation that is not traceable to a foreign Var */
  FDW_COLLATE_SAFE,   /* collation derives from a foreign Var */
  FDW_COLLATE_UNSAFE, /* collation is non-default and derives from something other than a foreign Var*/
} FDWCollateState;

typedef struct foreign_loc_cxt {
  Oid             collation;  /* OID of current collation, if any */
  FDWCollateState state;      /* state of current collation choice */
} foreign_loc_cxt;

/** Context for deparseExpr */
typedef struct deparse_expr_cxt {
  PlannerInfo* root;        /* global planner state */
  RelOptInfo*  foreignrel;  /* the foreign relation we are planning for */
  RelOptInfo*  scanrel;     /* the underlying scan relation. Same as foreignrel, when that represents a join or a base relation. */
  List**       params_list; /* exprs that will become remote Params */
  StringInfo   buf;         /* output buffer to append to */
} deparse_expr_cxt;

/** external prototypes */
extern short        c2dbType                  (short fcType);
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);
extern void*        db2alloc                  (const char* type, size_t size);
extern void*        db2strdup                 (const char* source);
extern void         db2free                   (void* p);
extern bool         is_shippable              (Oid objectId, Oid classId, DB2FdwState* fpinfo);
extern EquivalenceMember* find_em_for_rel(PlannerInfo *root, EquivalenceClass *ec, RelOptInfo *rel);

/** local prototypes */
bool                is_foreign_expr           (PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
static bool         foreign_expr_walker       (Node *node, foreign_glob_cxt *glob_cxt, foreign_loc_cxt *outer_cxt, foreign_loc_cxt *case_arg_cxt);
bool                is_foreign_param          (PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
bool                is_foreign_pathkey        (PlannerInfo *root, RelOptInfo *baserel, PathKey *pathkey);
void                appendAsType              (StringInfoData* dest, Oid type);
void                deparseFromExprForRel     (PlannerInfo* root, RelOptInfo* foreignrel, StringInfo buf, List** params_list);
char*               deparseWhereConditions    (PlannerInfo* root, RelOptInfo* rel);
char*               deparseExpr               (PlannerInfo* root, RelOptInfo* rel, Expr* expr, List** params);
static void         deparseExprInt            (Expr*              expr, deparse_expr_cxt* ctx);
static void         deparseConstExpr          (Const*             expr, deparse_expr_cxt* ctx);
static void         deparseParamExpr          (Param*             expr, deparse_expr_cxt* ctx);
static void         deparseVarExpr            (Var*               expr, deparse_expr_cxt* ctx);
static void         deparseOpExpr             (OpExpr*            expr, deparse_expr_cxt* ctx);
static void         deparseScalarArrayOpExpr  (ScalarArrayOpExpr* expr, deparse_expr_cxt* ctx);
static void         deparseDistinctExpr       (DistinctExpr*      expr, deparse_expr_cxt* ctx);
static void         deparseNullTest           (NullTest*          expr, deparse_expr_cxt* ctx);
static void         deparseNullIfExpr         (NullIfExpr*        expr, deparse_expr_cxt* ctx);
static void         deparseBoolExpr           (BoolExpr*          expr, deparse_expr_cxt* ctx);
static void         deparseCaseExpr           (CaseExpr*          expr, deparse_expr_cxt* ctx);
static void         deparseCoalesceExpr       (CoalesceExpr*      expr, deparse_expr_cxt* ctx);
static void         deparseFuncExpr           (FuncExpr*          expr, deparse_expr_cxt* ctx);
static void         deparseAggref             (Aggref*            expr, deparse_expr_cxt* ctx);
static void         deparseCoerceViaIOExpr    (CoerceViaIO*       expr, deparse_expr_cxt* ctx);
static void         deparseSQLValueFuncExpr   (SQLValueFunction*  expr, deparse_expr_cxt* ctx);
static char*        datumToString             (Datum datum, Oid type);
char*               deparseDate               (Datum datum);
char*               deparseTimestamp          (Datum datum, bool hasTimezone);
static char*        deparseInterval           (Datum datum);
static const char*  get_jointype_name         (JoinType jointype);

/** Returns true if given expr is safe to evaluate on the foreign server.
 */
bool is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr) {
  foreign_glob_cxt glob_cxt;
  foreign_loc_cxt  loc_cxt;
  DB2FdwState*     fpinfo  = (DB2FdwState*) (baserel->fdw_private);
  bool             bResult = false;

  /** Check that the expression consists of nodes that are safe to execute
   * remotely.
   */
  glob_cxt.root       = root;
  glob_cxt.foreignrel = baserel;

  /*
   * For an upper relation, use relids from its underneath scan relation,
   * because the upperrel's own relids currently aren't set to anything
   * meaningful by the core code.  For other relation, use their own relids.
   */
  glob_cxt.relids   = (IS_UPPER_REL(baserel)) ? fpinfo->outerrel->relids : baserel->relids;
  loc_cxt.collation = InvalidOid;
  loc_cxt.state     = FDW_COLLATE_NONE;
  if (foreign_expr_walker((Node*) expr, &glob_cxt, &loc_cxt, NULL)) {
    /** If the expression has a valid collation that does not arise from a
     * foreign var, the expression can not be sent over.
    */
    if (loc_cxt.state != FDW_COLLATE_UNSAFE) {
      /** An expression which includes any mutable functions can't be sent over
       * because its result is not stable.  For example, sending now() remote
       * side could cause confusion from clock offsets.  Future versions might
       * be able to make this choice with more granularity.  (We check this last
       * because it requires a lot of expensive catalog lookups.)
       */
      if (!contain_mutable_functions((Node *) expr)) {
        bResult = true;
      }
    }
  }
  /* OK to evaluate on the remote server */
  return bResult;
}

/** Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * case_arg_cxt is NULL if this subexpression is not inside a CASE-with-arg.
 * Otherwise, it points to the collation info derived from the arg expression,
 * which must be consulted by any CaseTestExpr.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (they are "shippable"),
 * and that all collations used in the expression derive from Vars of the
 * foreign table.  Because of the latter, the logic is pretty close to
 * assign_collations_walker() in parse_collate.c, though we can assume here
 * that the given expression is valid.  Note function mutability is not
 * currently considered here.
 */
static bool foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt, foreign_loc_cxt *outer_cxt, foreign_loc_cxt *case_arg_cxt) {
  bool            check_type  = true;
  DB2FdwState*    fpinfo      = NULL;
  foreign_loc_cxt inner_cxt;
  Oid             collation;
  FDWCollateState state;

  /* Need do nothing for empty subexpressions */
  if (node == NULL)
    return true;

  /* May need server info from baserel's fdw_private struct */
  fpinfo = (DB2FdwState*) glob_cxt->foreignrel->fdw_private;

  /* Set up inner_cxt for possible recursion to child nodes */
  inner_cxt.collation = InvalidOid;
  inner_cxt.state = FDW_COLLATE_NONE;

  switch (nodeTag(node)) {
    case T_Var: {
      Var		   *var = (Var *) node;
      /** If the Var is from the foreign table, we consider its
       * collation (if any) safe to use.  If it is from another
       * table, we treat its collation the same way as we would a
       * Param's collation, ie it's not safe for it to have a
       * non-default collation.
       */
      if (bms_is_member(var->varno, glob_cxt->relids) && var->varlevelsup == 0) {
        /* Var belongs to foreign table */
        /** System columns other than ctid should not be sent to
         * the remote, since we don't make any effort to ensure
         * that local and remote values match (tableoid, in
         * particular, almost certainly doesn't match).
         */
        if (var->varattno < 0 && var->varattno != SelfItemPointerAttributeNumber)
          return false;

        /* Else check the collation */
        collation = var->varcollid;
        state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
      } else {
        /* Var belongs to some other table */
        collation = var->varcollid;
        if (collation == InvalidOid || collation == DEFAULT_COLLATION_OID) {
          /** It's noncollatable, or it's safe to combine with a
           * collatable foreign Var, so set state to NONE.
           */
          state = FDW_COLLATE_NONE;
        } else {
          /** Do not fail right away, since the Var might appear
           * in a collation-insensitive context.
           */
          state = FDW_COLLATE_UNSAFE;
        }
      }
    }
    break;
    case T_Const: {
      Const* c = (Const *) node;

      /** Constants of regproc and related types can't be shipped
       * unless the referenced object is shippable.  But NULL's ok.
       * (See also the related code in dependency.c.)
       */
      if (!c->constisnull) {
        switch (c->consttype) {
          case REGPROCOID:
          case REGPROCEDUREOID:
            if (!is_shippable(DatumGetObjectId(c->constvalue), ProcedureRelationId, fpinfo))
              return false;
          break;
          case REGOPEROID:
          case REGOPERATOROID:
            if (!is_shippable(DatumGetObjectId(c->constvalue), OperatorRelationId, fpinfo))
              return false;
          break;
          case REGCLASSOID:
            if (!is_shippable(DatumGetObjectId(c->constvalue), RelationRelationId, fpinfo))
              return false;
          break;
          case REGTYPEOID:
            if (!is_shippable(DatumGetObjectId(c->constvalue), TypeRelationId, fpinfo))
              return false;
          break;
          case REGCOLLATIONOID:
            if (!is_shippable(DatumGetObjectId(c->constvalue), CollationRelationId, fpinfo))
              return false;
          break;
          case REGCONFIGOID:
            /** For text search objects only, we weaken the normal shippability criterion to 
             *  allow all OIDs below FirstNormalObjectId.  Without this, none of the initdb-installed
             *  TS configurations would be shippable, which would be quite annoying.
             */
            if (DatumGetObjectId(c->constvalue) >= FirstNormalObjectId && !is_shippable(DatumGetObjectId(c->constvalue), TSConfigRelationId, fpinfo))
              return false;
          break;
          case REGDICTIONARYOID:
            if (DatumGetObjectId(c->constvalue) >= FirstNormalObjectId && !is_shippable(DatumGetObjectId(c->constvalue), TSDictionaryRelationId, fpinfo))
              return false;
          break;
          case REGNAMESPACEOID:
            if (!is_shippable(DatumGetObjectId(c->constvalue), NamespaceRelationId, fpinfo))
              return false;
          break;
          case REGROLEOID:
            if (!is_shippable(DatumGetObjectId(c->constvalue), AuthIdRelationId, fpinfo))
              return false;
          break;
          #ifdef REGDATABASEOID
          case REGDATABASEOID:
            if (!is_shippable(DatumGetObjectId(c->constvalue), DatabaseRelationId, fpinfo))
              return false;
          break;
          #endif
        }
      }

      /** If the constant has nondefault collation, either it's of a
       * non-builtin type, or it reflects folding of a CollateExpr.
       * It's unsafe to send to the remote unless it's used in a
       * non-collation-sensitive context.
       */
      collation = c->constcollid;
      state     = (collation == InvalidOid || collation == DEFAULT_COLLATION_OID) ? FDW_COLLATE_NONE : FDW_COLLATE_UNSAFE;
    }
    break;
    case T_Param: {
      Param	   *p = (Param *) node;

      /** If it's a MULTIEXPR Param, punt.  We can't tell from here whether the referenced sublink/subplan contains any remote
       *  Vars; if it does, handling that is too complicated to consider supporting at present.  Fortunately, MULTIEXPR
       *  Params are not reduced to plain PARAM_EXEC until the end of planning, so we can easily detect this case.  (Normal
       *  PARAM_EXEC Params are safe to ship because their values come from somewhere else in the plan tree; but a MULTIEXPR
       *  references a sub-select elsewhere in the same targetlist, so we'd be on the hook to evaluate it somehow if we wanted
       *  to handle such cases as direct foreign updates.)
       */
      if (p->paramkind == PARAM_MULTIEXPR)
        return false;

      /** Collation rule is same as for Consts and non-foreign Vars. */
      collation = p->paramcollid;
      state     = (collation == InvalidOid || collation == DEFAULT_COLLATION_OID) ? FDW_COLLATE_NONE : FDW_COLLATE_UNSAFE;
    }
    break;
		case T_SubscriptingRef:
			{
				SubscriptingRef *sr = (SubscriptingRef *) node;

				/* Assignment should not be in restrictions. */
				if (sr->refassgnexpr != NULL)
					return false;

				/*
				 * Recurse into the remaining subexpressions.  The container
				 * subscripts will not affect collation of the SubscriptingRef
				 * result, so do those first and reset inner_cxt afterwards.
				 */
				if (!foreign_expr_walker((Node *) sr->refupperindexpr,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;
				inner_cxt.collation = InvalidOid;
				inner_cxt.state = FDW_COLLATE_NONE;
				if (!foreign_expr_walker((Node *) sr->reflowerindexpr,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;
				inner_cxt.collation = InvalidOid;
				inner_cxt.state = FDW_COLLATE_NONE;
				if (!foreign_expr_walker((Node *) sr->refexpr,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * Container subscripting typically yields same collation as
				 * refexpr's, but in case it doesn't, use same logic as for
				 * function nodes.
				 */
				collation = sr->refcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;

				/*
				 * If function used by the expression is not shippable, it
				 * can't be sent to remote because it might have incompatible
				 * semantics on remote side.
				 */
				if (!is_shippable(fe->funcid, ProcedureRelationId, fpinfo))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) fe->args,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * If function's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (fe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 fe->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = fe->funccollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
			{
				OpExpr	   *oe = (OpExpr *) node;

				/*
				 * Similarly, only shippable operators can be sent to remote.
				 * (If the operator is shippable, we assume its underlying
				 * function is too.)
				 */
				if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Result-collation handling is same as for functions */
				collation = oe->opcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

				/*
				 * Again, only shippable operators can be sent to remote.
				 */
				if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!foreign_expr_walker((Node *) r->arg,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * RelabelType must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a real function).
				 */
				collation = r->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ArrayCoerceExpr:
			{
				ArrayCoerceExpr *e = (ArrayCoerceExpr *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!foreign_expr_walker((Node *) e->arg,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * T_ArrayCoerceExpr must not introduce a collation not
				 * derived from an input foreign Var (same logic as for a
				 * function).
				 */
				collation = e->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) nt->arg,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_CaseExpr:
			{
				CaseExpr   *ce = (CaseExpr *) node;
				foreign_loc_cxt arg_cxt;
				foreign_loc_cxt tmp_cxt;
				ListCell   *lc;

				/*
				 * Recurse to CASE's arg expression, if any.  Its collation
				 * has to be saved aside for use while examining CaseTestExprs
				 * within the WHEN expressions.
				 */
				arg_cxt.collation = InvalidOid;
				arg_cxt.state = FDW_COLLATE_NONE;
				if (ce->arg)
				{
					if (!foreign_expr_walker((Node *) ce->arg,
											 glob_cxt, &arg_cxt, case_arg_cxt))
						return false;
				}

				/* Examine the CaseWhen subexpressions. */
				foreach(lc, ce->args)
				{
					CaseWhen   *cw = lfirst_node(CaseWhen, lc);

					if (ce->arg)
					{
						/*
						 * In a CASE-with-arg, the parser should have produced
						 * WHEN clauses of the form "CaseTestExpr = RHS",
						 * possibly with an implicit coercion inserted above
						 * the CaseTestExpr.  However in an expression that's
						 * been through the optimizer, the WHEN clause could
						 * be almost anything (since the equality operator
						 * could have been expanded into an inline function).
						 * In such cases forbid pushdown, because
						 * deparseCaseExpr can't handle it.
						 */
						Node	   *whenExpr = (Node *) cw->expr;
						List	   *opArgs;

						if (!IsA(whenExpr, OpExpr))
							return false;

						opArgs = ((OpExpr *) whenExpr)->args;
						if (list_length(opArgs) != 2 ||
							!IsA(strip_implicit_coercions(linitial(opArgs)),
								 CaseTestExpr))
							return false;
					}

					/*
					 * Recurse to WHEN expression, passing down the arg info.
					 * Its collation doesn't affect the result (really, it
					 * should be boolean and thus not have a collation).
					 */
					tmp_cxt.collation = InvalidOid;
					tmp_cxt.state = FDW_COLLATE_NONE;
					if (!foreign_expr_walker((Node *) cw->expr,
											 glob_cxt, &tmp_cxt, &arg_cxt))
						return false;

					/* Recurse to THEN expression. */
					if (!foreign_expr_walker((Node *) cw->result,
											 glob_cxt, &inner_cxt, case_arg_cxt))
						return false;
				}

				/* Recurse to ELSE expression. */
				if (!foreign_expr_walker((Node *) ce->defresult,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)  This is the same as for function
				 * nodes, except that the input collation is derived from only
				 * the THEN and ELSE subexpressions.
				 */
				collation = ce->casecollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_CaseTestExpr:
			{
				CaseTestExpr *c = (CaseTestExpr *) node;

				/* Punt if we seem not to be inside a CASE arg WHEN. */
				if (!case_arg_cxt)
					return false;

				/*
				 * Otherwise, any nondefault collation attached to the
				 * CaseTestExpr node must be derived from foreign Var(s) in
				 * the CASE arg.
				 */
				collation = c->collation;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (case_arg_cxt->state == FDW_COLLATE_SAFE &&
						 collation == case_arg_cxt->collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *a = (ArrayExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) a->elements,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * ArrayExpr must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a function).
				 */
				collation = a->array_collid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt, case_arg_cxt))
						return false;
				}

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */
				collation = inner_cxt.collation;
				state = inner_cxt.state;

				/* Don't apply exprType() to the list. */
				check_type = false;
			}
			break;
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				ListCell   *lc;

				/* Not safe to pushdown when not in grouping context */
				if (!IS_UPPER_REL(glob_cxt->foreignrel))
					return false;

				/* Only non-split aggregates are pushable. */
				if (agg->aggsplit != AGGSPLIT_SIMPLE)
					return false;

				/* As usual, it must be shippable. */
				if (!is_shippable(agg->aggfnoid, ProcedureRelationId, fpinfo))
					return false;

				/*
				 * Recurse to input args. aggdirectargs, aggorder and
				 * aggdistinct are all present in args, so no need to check
				 * their shippability explicitly.
				 */
				foreach(lc, agg->args)
				{
					Node	   *n = (Node *) lfirst(lc);

					/* If TargetEntry, extract the expression from it */
					if (IsA(n, TargetEntry))
					{
						TargetEntry *tle = (TargetEntry *) n;

						n = (Node *) tle->expr;
					}

					if (!foreign_expr_walker(n,
											 glob_cxt, &inner_cxt, case_arg_cxt))
						return false;
				}

				/*
				 * For aggorder elements, check whether the sort operator, if
				 * specified, is shippable or not.
				 */
				if (agg->aggorder)
				{
					foreach(lc, agg->aggorder)
					{
						SortGroupClause *srt = (SortGroupClause *) lfirst(lc);
						Oid			sortcoltype;
						TypeCacheEntry *typentry;
						TargetEntry *tle;

						tle = get_sortgroupref_tle(srt->tleSortGroupRef,
												   agg->args);
						sortcoltype = exprType((Node *) tle->expr);
						typentry = lookup_type_cache(sortcoltype,
													 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
						/* Check shippability of non-default sort operator. */
						if (srt->sortop != typentry->lt_opr &&
							srt->sortop != typentry->gt_opr &&
							!is_shippable(srt->sortop, OperatorRelationId,
										  fpinfo))
							return false;
					}
				}

				/* Check aggregate filter */
				if (!foreign_expr_walker((Node *) agg->aggfilter,
										 glob_cxt, &inner_cxt, case_arg_cxt))
					return false;

				/*
				 * If aggregate's input collation is not derived from a
				 * foreign Var, it can't be sent to remote.
				 */
				if (agg->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 agg->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = agg->aggcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
    default:
      /** If it's anything else, assume it's unsafe.  This list can be
       * expanded later, but don't forget to add deparse support below.
       */
      return false;
  }

  /** If result type of given expression is not shippable, it can't be sent
   * to remote because it might have incompatible semantics on remote side.
   */
  if (check_type && !is_shippable(exprType(node), TypeRelationId, fpinfo))
    return false;

  /** Now, merge my collation information into my parent's state. */
  if (state > outer_cxt->state) {
    /* Override previous parent state */
    outer_cxt->collation = collation;
    outer_cxt->state = state;
  }	else if (state == outer_cxt->state) {
    /* Merge, or detect error if there's a collation conflict */
    switch (state) {
      case FDW_COLLATE_NONE: {
        /* Nothing + nothing is still nothing */
      }
      break;
      case FDW_COLLATE_SAFE: {
        if (collation != outer_cxt->collation) {
          /** Non-default collation always beats default.*/
          if (outer_cxt->collation == DEFAULT_COLLATION_OID) {
            /* Override previous parent state */
            outer_cxt->collation = collation;
          } else if (collation != DEFAULT_COLLATION_OID) {
            /** Conflict; show state as indeterminate.  We don't
             * want to "return false" right away, since parent
             * node might not care about collation.
             */
            outer_cxt->state = FDW_COLLATE_UNSAFE;
          }
        }
      }
      break;
      case FDW_COLLATE_UNSAFE: {
        /* We're still conflicted ... */
      }
      break;
    }
  }
  /* It looks OK */
  return true;
}

/** Returns true if given expr is something we'd have to send the value of
 * to the foreign server.
 *
 * This should return true when the expression is a shippable node that
 * deparseExpr would add to context->params_list.  Note that we don't care
 * if the expression *contains* such a node, only whether one appears at top
 * level.  We need this to detect cases where setrefs.c would recognize a
 * false match between an fdw_exprs item (which came from the params_list)
 * and an entry in fdw_scan_tlist (which we're considering putting the given
 * expression into).
 */
bool is_foreign_param(PlannerInfo *root, RelOptInfo *baserel, Expr *expr) {
  bool bResult = false;
  if (expr != NULL) {
    switch (nodeTag(expr)) {
      case T_Var: {
          /* It would have to be sent unless it's a foreign Var */
          Var*          var    = (Var *) expr;
          DB2FdwState*  fpinfo = (DB2FdwState*) (baserel->fdw_private);
          Relids        relids;

          relids  = (IS_UPPER_REL(baserel)) ? fpinfo->outerrel->relids : baserel->relids;
          bResult = !(bms_is_member(var->varno, relids) && var->varlevelsup == 0);
      }
      break;
      case T_Param:
        /* Params always have to be sent to the foreign server */
        bResult = true;
      default:
      break;
    }
  }
  return bResult;
}

/** Returns true if it's safe to push down the sort expression described by
 * 'pathkey' to the foreign server.
 */
bool is_foreign_pathkey(PlannerInfo *root, RelOptInfo *baserel, PathKey *pathkey) {
  EquivalenceClass* pathkey_ec = pathkey->pk_eclass;
  DB2FdwState*      fpinfo     = (DB2FdwState*) baserel->fdw_private;
  bool              bResult    = false;

  /** is_foreign_expr would detect volatile expressions as well, but checking
   * ec_has_volatile here saves some cycles.
   */
  if (!pathkey_ec->ec_has_volatile) {
    /* can't push down the sort if the pathkey's opfamily is not shippable */
    if (is_shippable(pathkey->pk_opfamily, OperatorFamilyRelationId, fpinfo)) {
      /* can push if a suitable EC member exists */
      bResult = (find_em_for_rel(root, pathkey_ec, baserel) != NULL);
    }
  }
  return bResult;
}

/** appendAsType
 *   Append "s" to "dest", adding appropriate casts for datetime "type".
 */
void appendAsType (StringInfoData* dest, Oid type) {
  db2Debug1("> %s::appendAsType", __FILE__);
  db2Debug2("  dest->data: '%s'",dest->data);
  db2Debug2("  type: %d",type);
  switch (type) {
    case DATEOID:
      appendStringInfo (dest, "CAST (? AS DATE)");
      break;
    case TIMESTAMPOID:
      appendStringInfo (dest, "CAST (? AS TIMESTAMP)");
      break;
    case TIMESTAMPTZOID:
      appendStringInfo (dest, "CAST (? AS TIMESTAMP)");
      break;
    case TIMEOID:
      appendStringInfo (dest, "(CAST (? AS TIME))");
      break;
    case TIMETZOID:
      appendStringInfo (dest, "(CAST (? AS TIME))");
      break;
    default:
      appendStringInfo (dest, "?");
    break;
  }
  db2Debug2("  dest->data: '%s'", dest->data);
  db2Debug1("< %s::appendAsType", __FILE__);
}

/** This macro is used by deparseExpr to identify PostgreSQL
 * types that can be translated to DB2 SQL.
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
      || (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
      || (x) == INT4OID || (x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID \
      || (x) == NUMERICOID || (x) == DATEOID || (x) == TIMEOID || (x) == TIMESTAMPOID \
      || (x) == TIMESTAMPTZOID || (x) == INTERVALOID)

/** deparseFromExprForRel
 *   Construct FROM clause for given relation.
 *   The function constructs ... JOIN ... ON ... for join relation. For a base
 *   relation it just returns the table name.
 *   All tables get an alias based on the range table index.
 */
void deparseFromExprForRel (PlannerInfo* root, RelOptInfo* foreignrel, StringInfo buf, List** params_list) {
  DB2FdwState* fdwState = (DB2FdwState*) foreignrel->fdw_private;

  db2Debug1("> deparseFromExprForRel");
  db2Debug2("  buf: '%s",buf->data);
  if (IS_SIMPLE_REL (foreignrel)) {
    appendStringInfo (buf, "%s", fdwState->db2Table->name);
    appendStringInfo (buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);
  } else {
    /* join relation */
    RelOptInfo*    rel_o      = fdwState->outerrel;
    RelOptInfo*    rel_i      = fdwState->innerrel;
    StringInfoData join_sql_o;
    StringInfoData join_sql_i;
    ListCell*      lc         = NULL;
    bool           is_first   = true;
    char*          where      = NULL;

    /* Deparse outer relation */
    initStringInfo (&join_sql_o);
    deparseFromExprForRel (root, rel_o, &join_sql_o, params_list);

    /* Deparse inner relation */
    initStringInfo (&join_sql_i);
    deparseFromExprForRel (root, rel_i, &join_sql_i, params_list);

    // For a join relation FROM clause entry is deparsed as (outer relation) <join type> (inner relation) ON joinclauses
    appendStringInfo (buf, "(%s %s JOIN %s ON ", join_sql_o.data, get_jointype_name (fdwState->jointype), join_sql_i.data);

    /* we can only get here if the join is pushed down, so there are join clauses */
    Assert (fdwState->joinclauses);

    foreach (lc, fdwState->joinclauses) {
        Expr *expr = (Expr *)lfirst(lc);
        /* connect expressions with AND */
        if (!is_first)
            appendStringInfo(buf, " AND ");
        /* deparse and append a join condition */
        where = deparseExpr(root, foreignrel, expr, params_list);
        appendStringInfo(buf, "%s", where);
        is_first = false;
    }
    /* End the FROM clause entry. */
    appendStringInfo (buf, ")");
  }
  db2Debug2("  buf: '%s'",buf->data);
  db2Debug1("< deparseFromExprForRel");
}

/** deparseWhereConditions
 *   Classify conditions into remote_conds or local_conds.
 *   Those conditions that can be pushed down will be collected into
 *   an DB2 WHERE clause that is returned.
 */
char* deparseWhereConditions (PlannerInfo* root, RelOptInfo * rel) {
  List*          conditions = rel->baserestrictinfo;
  DB2FdwState*   fdwState   = (DB2FdwState*) rel->fdw_private;
  ListCell*      cell;
  char*          where;
  char*          keyword = "WHERE";
  StringInfoData where_clause;

  db2Debug1("> deparseWhereCondition");
  initStringInfo (&where_clause);
  foreach (cell, conditions) {
    /* check if the condition can be pushed down */
    where = deparseExpr (root, rel, ((RestrictInfo*) lfirst (cell))->clause, &(fdwState->params));
    if (where != NULL) {
      fdwState->remote_conds = lappend (fdwState->remote_conds, ((RestrictInfo*) lfirst (cell))->clause);

      /* append new WHERE clause to query string */
      appendStringInfo (&where_clause, " %s %s", keyword, where);
      keyword = "AND";
      db2free (where);
    } else {
      fdwState->local_conds = lappend (fdwState->local_conds, ((RestrictInfo*) lfirst (cell))->clause);
    }
  }
  db2Debug1("< deparseWhereCondition : %s",where_clause.data);
  return where_clause.data;
}

/** deparseExpr
 *   Create and return an DB2 SQL string from "expr".
 *   Returns NULL if that is not possible, else an allocated string.
 *   As a side effect, all Params incorporated in the WHERE clause
 *   will be stored in "params".
 */
char* deparseExpr (PlannerInfo* root, RelOptInfo* rel, Expr* expr, List** params) {
  char* retValue = NULL;
  db2Debug1("> %s::deparseExpr", __FILE__);
  if (expr != NULL) {
    deparse_expr_cxt* ctx = db2alloc("deparseExpr.context", sizeof(deparse_expr_cxt));
    StringInfoData    buf;

    initStringInfo(&buf);

    ctx->root        = root;
    ctx->foreignrel  = rel;
    ctx->scanrel     = rel;
    ctx->buf         = &buf;
    ctx->params_list = params;
    deparseExprInt(expr, ctx);
    retValue = (buf.len > 0) ? db2strdup(buf.data) : NULL;
    db2free(ctx->buf->data);
    db2free(ctx);
  }
  db2Debug1("< %s::deparseExpr: %s", __FILE__, retValue);
  return retValue;
}

static void deparseExprInt           (Expr*              expr, deparse_expr_cxt* ctx) {
  db2Debug1("> %s::deparseExprInt", __FILE__);
  db2Debug2("  expr: %x",expr);
  if (expr != NULL) {
    db2Debug2("  expr->type: %d",expr->type);
    switch (expr->type) {
      case T_Const: {
        deparseConstExpr((Const*)expr, ctx);
      }
      break;
      case T_Param: {
        deparseParamExpr((Param*) expr, ctx);
      }
      break;
      case T_Var: {
        deparseVarExpr ((Var*)expr, ctx);
      }
      break;
      case T_OpExpr: {
        deparseOpExpr ((OpExpr*)expr, ctx);
      }
      break;
      case T_ScalarArrayOpExpr: {
        deparseScalarArrayOpExpr ((ScalarArrayOpExpr*)expr, ctx);
      }
      break;
      case T_DistinctExpr: {
        deparseDistinctExpr ((DistinctExpr*)expr, ctx);
      }
      break;
      case T_NullIfExpr: {
        deparseNullIfExpr ((NullIfExpr*)expr, ctx);
      }
      break;
      case T_BoolExpr: {
        deparseBoolExpr ((BoolExpr*)expr, ctx);
      }
      break;
      case T_RelabelType: {
        deparseExprInt (((RelabelType*)expr)->arg, ctx);
      }
      break;
      case T_CoerceToDomain: {
        deparseExprInt (((CoerceToDomain*)expr)->arg, ctx);
      }
      break;
      case T_CaseExpr: {
        deparseCaseExpr ((CaseExpr*)expr, ctx);
      }
      break;
      case T_CoalesceExpr: {
        deparseCoalesceExpr ((CoalesceExpr*)expr, ctx);
      }
      break;
      case T_NullTest: {
        deparseNullTest((NullTest*) expr, ctx);
      }
      break;
      case T_FuncExpr: {
        deparseFuncExpr((FuncExpr*)expr, ctx);
      }
      break;
      case T_CoerceViaIO: {
        deparseCoerceViaIOExpr((CoerceViaIO*) expr, ctx);
      }
      break;
      case T_SQLValueFunction: {
        deparseSQLValueFuncExpr((SQLValueFunction*)expr, ctx);
      }
      break;
      case T_Aggref: {
        deparseAggref((Aggref*)expr, ctx);
      }
      break;
      default: {
        /* we cannot translate this to DB2 */
        db2Debug2("  expression cannot be translated to DB2", __FILE__);
      }
      break;
    }
  }
  db2Debug1("< %s::deparseExpr : %s", __FILE__, ctx->buf->data);
}

static void deparseConstExpr         (Const*             expr, deparse_expr_cxt* ctx) {
  db2Debug1("> %s::deparseConstExpr", __FILE__);
  if (expr->constisnull) {
    /* only translate NULLs of a type DB2 can handle */
    if (canHandleType (expr->consttype)) {
      appendStringInfo (ctx->buf, "NULL");
    }
  } else {
    /* get a string representation of the value */
    char* c = datumToString (expr->constvalue, expr->consttype);
    if (c != NULL) {
      appendStringInfo (ctx->buf, "%s", c);
    }
  }
  db2Debug1("< %s::deparseConstExpr", __FILE__);
}

static void deparseParamExpr         (Param*             expr, deparse_expr_cxt* ctx) {
  #ifdef OLD_FDW_API
  /* don't try to push down parameters with 9.1 */
  db2Debug1("> %s::deparseParamExpr", __FILE__);
  db2Debug2("  don't try to push down parameters with 9.1");
  #else
  ListCell* cell  = NULL;
  char      parname[10];

  db2Debug1("> %s::deparseParamExpr", __FILE__);
  /* don't try to handle interval parameters */
  if (!canHandleType (expr->paramtype) || expr->paramtype == INTERVALOID) {
    db2Debug2("  !canHhandleType(expr->paramtype %d) || rxpr->paramtype == INTERVALOID)", expr->paramtype);
  } else {
    /* find the index in the parameter list */
    int index = 0;
    foreach (cell, *(ctx->params_list)) {
      ++index;
      if (equal (expr, (Node*) lfirst (cell)))
        break;
    }
    if (cell == NULL) {
      /* add the parameter to the list */
      ++index;
      *(ctx->params_list) = lappend (*(ctx->params_list), expr);
    }
    /* parameters will be called :p1, :p2 etc. */
    snprintf (parname, 10, ":p%d", index);
    appendAsType (ctx->buf, expr->paramtype);
  }
  #endif /* OLD_FDW_API */
  db2Debug1("< %s::deparseParamExpr", __FILE__);
}

static void deparseVarExpr           (Var*               expr, deparse_expr_cxt* ctx) {
  const DB2Table*  var_table = NULL;  /* db2Table that belongs to a Var */

  db2Debug1("> %s::deparseVarExpr", __FILE__);
  /* check if the variable belongs to one of our foreign tables */
  #ifdef JOIN_API
  if (IS_SIMPLE_REL (ctx->foreignrel)) {
  #endif /* JOIN_API */
    if (expr->varno == ctx->foreignrel->relid && expr->varlevelsup == 0)
      var_table = ((DB2FdwState*)ctx->foreignrel->fdw_private)->db2Table;
  #ifdef JOIN_API
  } else {
    DB2FdwState* joinstate  = (DB2FdwState*) ctx->foreignrel->fdw_private;
    DB2FdwState* outerstate = (DB2FdwState*) joinstate->outerrel->fdw_private;
    DB2FdwState* innerstate = (DB2FdwState*) joinstate->innerrel->fdw_private;
    /* we can't get here if the foreign table has no columns, so this is safe */
    if (expr->varno == outerstate->db2Table->cols[0]->varno && expr->varlevelsup == 0)
      var_table = outerstate->db2Table;
    if (expr->varno == innerstate->db2Table->cols[0]->varno && expr->varlevelsup == 0)
      var_table = innerstate->db2Table;
  }
  #endif /* JOIN_API */
  if (var_table) {
    /* the variable belongs to a foreign table, replace it with the name */
    /* we cannot handle system columns */
    db2Debug2("  varattno: %d",expr->varattno);
    if (expr->varattno > 0) {
      /** Allow boolean columns here.
       * They will be rendered as ("COL" <> 0).
       */
      if (!(canHandleType (expr->vartype) || expr->vartype == BOOLOID)) {
        db2Debug2("  !(canHandleType (vartype %d) || vartype == BOOLOID",expr->vartype);
      } else {
        /* get var_table column index corresponding to this column (-1 if none) */
        int index = var_table->ncols - 1;
        while (index >= 0 && var_table->cols[index]->pgattnum != expr->varattno) {
          --index;
        }
        /* if no DB2 column corresponds, translate as NULL */
        if (index == -1) {
          appendStringInfo (ctx->buf, "NULL");
        } else {
          /** Don't try to convert a column reference if the type is
           * converted from a non-string type in DB2 to a string type
           * in PostgreSQL because functions and operators won't work the same.
           */
          short db2type = c2dbType(var_table->cols[index]->colType);
          db2Debug2("  db2type: %d", db2type);
          if ((expr->vartype == TEXTOID || expr->vartype == BPCHAROID || expr->vartype == VARCHAROID)  && db2type != DB2_VARCHAR && db2type != DB2_CHAR) {
            db2Debug2("  vartype: %d", expr->vartype);
          } else {
            /* work around the lack of booleans in DB2 */
            if (expr->vartype == BOOLOID) {
              appendStringInfo (ctx->buf, "(");
            }
            /* qualify with an alias based on the range table index */
            appendStringInfo(ctx->buf, "%s%d.%s", "r", var_table->cols[index]->varno, var_table->cols[index]->colName);
            /* work around the lack of booleans in DB2 */
            if (expr->vartype == BOOLOID) {
              appendStringInfo (ctx->buf, " <> 0)");
            }
          }
        }
      }
    }
  } else {
    #ifdef OLD_FDW_API
    // treat it like a parameter
    // don't try to push down parameters with 9.1
    db2Debug2("  don't try to push down parameters with 9.1");
    #else
    // don't try to handle type interval
    if (!canHandleType (expr->vartype) || expr->vartype == INTERVALOID) {
      db2Debug2("  !canHandleType (vartype %d) || vartype == INTERVALOID", expr->vartype);
    } else {
      ListCell*      cell   = NULL;
      int            index  = 0;

      /* find the index in the parameter list */
      foreach (cell, *(ctx->params_list)) {
        ++index;
        if (equal (expr, (Node*) lfirst (cell)))
          break;
      }
      if (cell == NULL) {
        /* add the parameter to the list */
        ++index;
        *(ctx->params_list) = lappend (*(ctx->params_list), expr);
      }
      /* parameters will be called :p1, :p2 etc. */
      appendStringInfo (ctx->buf, ":p%d", index);
    }
    #endif /* OLD_FDW_API */
  }
  db2Debug1("< %s::deparseVarExpr", __FILE__);
}

static void deparseOpExpr            (OpExpr*            expr, deparse_expr_cxt* ctx) {
  char*     opername    = NULL;
  char      oprkind     = 0x00;
  Oid       rightargtype= 0;
  Oid       leftargtype = 0;
  Oid       schema      = 0;
  HeapTuple tuple       ;

  db2Debug1("> %s::deparseOpExpr", __FILE__);
  /* get operator name, kind, argument type and schema */
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (expr->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", expr->opno);
  }
  opername     = db2strdup (((Form_pg_operator) GETSTRUCT (tuple))->oprname.data);
  oprkind      = ((Form_pg_operator) GETSTRUCT (tuple))->oprkind;
  leftargtype  = ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  schema       = ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
  ReleaseSysCache (tuple);
  /* ignore operators in other than the pg_catalog schema */
  if (schema != PG_CATALOG_NAMESPACE) {
    db2Debug2("  schema != PG_CATALOG_NAMESPACE");
  } else {
    if (!canHandleType (rightargtype)) {
      db2Debug2("  !canHandleType rightargtype(%d)", rightargtype);
    } else {
      /** Don't translate operations on two intervals.
     * INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND don't mix well.
     */
      if (leftargtype == INTERVALOID && rightargtype == INTERVALOID) {
        db2Debug2("  leftargtype == INTERVALOID && rightargtype == INTERVALOID");
      } else {
        /* the operators that we can translate */
        if ((strcmp (opername, ">")    == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID    && rightargtype != NAMEOID && rightargtype != CHAROID)
        ||  (strcmp (opername, "<")    == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID    && rightargtype != NAMEOID && rightargtype != CHAROID)
        ||  (strcmp (opername, ">=")   == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID    && rightargtype != NAMEOID && rightargtype != CHAROID)
        ||  (strcmp (opername, "<=")   == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID    && rightargtype != NAMEOID && rightargtype != CHAROID)
        ||  (strcmp (opername, "-")    == 0 && rightargtype != DATEOID && rightargtype != TIMESTAMPOID && rightargtype != TIMESTAMPTZOID)
        ||   strcmp (opername, "=")    == 0 || strcmp (opername, "<>")  == 0 || strcmp (opername, "+")   == 0 ||   strcmp (opername, "*")    == 0 
        ||   strcmp (opername, "~~")   == 0 || strcmp (opername, "!~~") == 0 || strcmp (opername, "~~*") == 0 ||   strcmp (opername, "!~~*") == 0 
        ||   strcmp (opername, "^")    == 0 || strcmp (opername, "%")   == 0 || strcmp (opername, "&")   == 0 ||   strcmp (opername, "|/")   == 0
        ||   strcmp (opername, "@")  == 0) {
          char* left = NULL;

          left = deparseExpr (ctx->root, ctx->foreignrel, linitial(expr->args), ctx->params_list);
          db2Debug2("  left: %s", left);
          if (left != NULL) {
            if (oprkind == 'b') {
              /* binary operator */
              char* right = NULL;

              right = deparseExpr (ctx->root, ctx->foreignrel, lsecond(expr->args), ctx->params_list);
              db2Debug2("  right: %s", right);
              if (right != NULL) {
                if (strcmp (opername, "~~") == 0) {
                  appendStringInfo (ctx->buf, "(%s LIKE %s ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "!~~") == 0) {
                  appendStringInfo (ctx->buf, "(%s NOT LIKE %s ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "~~*") == 0) {
                  appendStringInfo (ctx->buf, "(UPPER(%s) LIKE UPPER(%s) ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "!~~*") == 0) {
                  appendStringInfo (ctx->buf, "(UPPER(%s) NOT LIKE UPPER(%s) ESCAPE '\\')", left, right);
                } else if (strcmp (opername, "^") == 0) {
                  appendStringInfo (ctx->buf, "POWER(%s, %s)", left, right);
                } else if (strcmp (opername, "%") == 0) {
                  appendStringInfo (ctx->buf, "MOD(%s, %s)", left, right);
                } else if (strcmp (opername, "&") == 0) {
                  appendStringInfo (ctx->buf, "BITAND(%s, %s)", left, right);
                } else {
                  /* the other operators have the same name in DB2 */
                  appendStringInfo (ctx->buf, "(%s %s %s)", left, opername, right);
                }
                db2free(right);
              }
            } else {
              /* unary operator */
              if (strcmp (opername, "|/") == 0) {
                appendStringInfo (ctx->buf, "SQRT(%s)", left);
              } else if (strcmp (opername, "@") == 0) {
                appendStringInfo (ctx->buf, "ABS(%s)", left);
              } else {
                /* unary + or - */
                appendStringInfo (ctx->buf, "(%s%s)", opername, left);
              }
            }
            db2free(left);
          }
        } else {
          /* cannot translate this operator */
          db2Debug2("  cannot translate this opername: %s", opername);
        }
      }
    }
  }
  db2free (opername);
  db2Debug1("< %s::deparseOpExpr", __FILE__);
}

static void deparseScalarArrayOpExpr (ScalarArrayOpExpr* expr, deparse_expr_cxt* ctx) {
  char*              opername;
  Oid                leftargtype;
  Oid                schema;
  HeapTuple          tuple;

  db2Debug1("> %s::deparseScalarArrayOpExpr", __FILE__);
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum (expr->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", expr->opno);
  }
  opername    = db2strdup(((Form_pg_operator) GETSTRUCT (tuple))->oprname.data);
  leftargtype =           ((Form_pg_operator) GETSTRUCT (tuple))->oprleft;
  schema      =           ((Form_pg_operator) GETSTRUCT (tuple))->oprnamespace;
  ReleaseSysCache (tuple);
  /* get the type's output function */
  tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (leftargtype));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for type %u", leftargtype);
  }
  ReleaseSysCache (tuple);
  /* ignore operators in other than the pg_catalog schema */
  if (schema != PG_CATALOG_NAMESPACE) {
    db2Debug2("  schema != PG_CATALOG_NAMESPACE");
  } else {
    /* don't try to push down anything but IN and NOT IN expressions */
    if ((strcmp (opername, "=") != 0 || !expr->useOr) && (strcmp (opername, "<>") != 0 || expr->useOr)) {
      db2Debug2("  don't try to push down anything but IN and NOT IN expressions");
    } else {
      if (!canHandleType (leftargtype)) {
        db2Debug2("  cannot Handle Type leftargtype (%d)", leftargtype);
      } else {
        char* left  = NULL;
        char* right = NULL;

        left = deparseExpr (ctx->root, ctx->foreignrel,linitial (expr->args), ctx->params_list);
        // check if anything has been added beyond the initial "("
        if (left != NULL) {
          Expr* rightexpr = NULL;
          bool  bResult   = true;

          /* the second (=last) argument can be Const, ArrayExpr or ArrayCoerceExpr */
          rightexpr = (Expr*)llast(expr->args);
          switch (rightexpr->type) {
            case T_Const: {
              StringInfoData buf;
              /* the second (=last) argument is a Const of ArrayType */
              Const* constant = (Const*) rightexpr;
              /* using NULL in place of an array or value list is valid in DB2 and PostgreSQL */
              initStringInfo(&buf);
              if (constant->constisnull) {
                appendStringInfo(&buf, "NULL");
                right = db2strdup(buf.data);
              } else {
                Datum          datum;
                bool           isNull;
                ArrayIterator  iterator = array_create_iterator (DatumGetArrayTypeP (constant->constvalue), 0);
                bool           first_arg = true;

                /* loop through the array elements */
                while (array_iterate (iterator, &datum, &isNull)) {
                  char *c;
                  if (isNull) {
                    c = "NULL";
                  } else {
                    c = datumToString (datum, leftargtype);
                    db2Debug2("  c: %s",c);
                    if (c == NULL) {
                      array_free_iterator (iterator);
                      bResult = false;
                      break;
                    }
                  }
                  /* append the argument */
                  appendStringInfo (&buf, "%s%s", first_arg ? "" : ", ", c);
                  first_arg = false;
                }
                array_free_iterator (iterator);
                db2Debug2("  first_arg: %s", first_arg ? "true":"false");
                if (first_arg) {
                  // don't push down empty arrays
                  // since the semantics for NOT x = ANY(<empty array>) differ
                  bResult = false;
                }
                if (bResult) {
                  right = db2strdup(buf.data);
                }
              }
              db2free(buf.data);
            }
            break;
            case T_ArrayCoerceExpr: {
              /* the second (=last) argument is an ArrayCoerceExpr */
              ArrayCoerceExpr* arraycoerce = (ArrayCoerceExpr *) rightexpr;
              /* if the conversion requires more than binary coercion, don't push it down */
              if (arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType) {
                db2Debug2(" arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType");
                bResult = false;
                break;
              }
              /* the actual array is here */
              rightexpr = arraycoerce->arg;
            }
            /* fall through ! */
            case T_ArrayExpr: {
              /* the second (=last) argument is an ArrayExpr */
              StringInfoData buf;
              char*          element   = NULL;
              ArrayExpr*     array     = (ArrayExpr*) rightexpr;
              ListCell*      cell      = NULL;
              bool           first_arg = true;
              
              initStringInfo(&buf);
              /* loop the array arguments */
              foreach (cell, array->elements) {
                element = deparseExpr (ctx->root, ctx->foreignrel, (Expr*) lfirst (cell), ctx->params_list);
                if (element == NULL) {
                  /* if any element cannot be converted, give up */
                  db2free(buf.data);
                  bResult = false;
                  break;
                }
                appendStringInfo(&buf,"%s%s",(first_arg) ? "": ", ",element);
                first_arg = false;
              }
              db2Debug2("  first_arg: %s", first_arg ? "true" : "false");
              if (first_arg) {
                /* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
                db2free(buf.data);
                bResult = false;
                break;
              }
              right = (bResult) ? db2strdup(buf.data) : NULL;
              db2free(buf.data);
            }
            break;
            default: {
              db2Debug2("  rightexpr->type(%d) default ",rightexpr->type);
              bResult = false;
            }
            break;
          }
          // only when there is a usable result otherwise keep value to null
          if (bResult) {
            appendStringInfo (ctx->buf, "(%s %s IN (%s))",left, expr->useOr ? "" : "NOT", right);
          }
          db2free(left);
          db2free(right);
        }
      }
    }
  }
  db2Debug1("< %s::deparseScalarArrayOpExpr", __FILE__);
}

static void deparseDistinctExpr      (DistinctExpr*      expr, deparse_expr_cxt* ctx) {
  Oid       rightargtype = 0;
  HeapTuple tuple;

  db2Debug1("> %s::deparseDistinctExpr", __FILE__);
  tuple = SearchSysCache1 (OPEROID, ObjectIdGetDatum ((expr)->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", (expr)->opno);
  }
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  ReleaseSysCache (tuple);
  if (!canHandleType (rightargtype)) {
    db2Debug2(" cannot Handle Type rightargtype (%d)",rightargtype);
  } else {
    char* left  = NULL;

    left = deparseExpr (ctx->root, ctx->foreignrel, linitial ((expr)->args), ctx->params_list);
    if (left != NULL) {
      char* right = NULL;

      right = deparseExpr (ctx->root, ctx->foreignrel, lsecond ((expr)->args), ctx->params_list);
      if (right != NULL) {
        appendStringInfo (ctx->buf, "( %s IS DISTINCT FROM %s)", left, right);
      }
      db2free(right);
    }
    db2free(left);
  }
  db2Debug1("< %s::deparseDistinctExpr", __FILE__);
}

/** Deparse IS [NOT] NULL expression.
 */
static void deparseNullTest          (NullTest*          expr, deparse_expr_cxt* ctx) {
  StringInfo	buf = ctx->buf;

  db2Debug1("> %s::deparseNullTest", __FILE__);
  appendStringInfoChar(buf, '(');
  deparseExprInt (expr->arg, ctx);

  /** For scalar inputs, we prefer to print as IS [NOT] NULL, which is
   *  shorter and traditional.  If it's a rowtype input but we're applying a
   *  scalar test, must print IS [NOT] DISTINCT FROM NULL to be semantically
   *  correct.
   */
  if (expr->argisrow || !type_is_rowtype(exprType((Node *) expr->arg))) {
    if (expr->nulltesttype == IS_NULL)
      appendStringInfoString(buf, " IS NULL)");
    else
      appendStringInfoString(buf, " IS NOT NULL)");
  } else {
    if (expr->nulltesttype == IS_NULL)
      appendStringInfoString(buf, " IS NOT DISTINCT FROM NULL)");
    else
      appendStringInfoString(buf, " IS DISTINCT FROM NULL)");
  }
  db2Debug1("< %s::deparseNullTest : %s", __FILE__, buf->data);
}

static void deparseNullIfExpr        (NullIfExpr*        expr, deparse_expr_cxt* ctx) {
  Oid       rightargtype = 0;
  HeapTuple tuple;

  db2Debug1("> %s::deparseNullIfExpr", __FILE__);
  tuple        = SearchSysCache1 (OPEROID, ObjectIdGetDatum ((expr)->opno));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for operator %u", (expr)->opno);
  }
  rightargtype = ((Form_pg_operator) GETSTRUCT (tuple))->oprright;
  ReleaseSysCache (tuple);
  if (!canHandleType (rightargtype)) {
    db2Debug2("  cannot Handle Type rightargtype (%d)",rightargtype);
  } else {
    char* left = NULL;
    left = deparseExpr (ctx->root, ctx->foreignrel, linitial((expr)->args), ctx->params_list);

    if (left != NULL) {
      char* right = NULL;

      right = deparseExpr (ctx->root, ctx->foreignrel, lsecond((expr)->args), ctx->params_list);
      if (right != NULL) {
        appendStringInfo (ctx->buf, "NULLIF(%s,%s)", left, right);
      }
      db2free(right);
    }
    db2free(left);
  }
  db2Debug1("< %s::deparseNullIfExpr: %s", __FILE__, ctx->buf->data);
}

static void deparseBoolExpr          (BoolExpr*          expr, deparse_expr_cxt* ctx) {
  ListCell*         cell = NULL;
  char*             arg  = NULL;
  StringInfoData    buf;

  db2Debug1("> %s::deparseBoolExpr", __FILE__);
  initStringInfo(&buf);
  arg = deparseExpr (ctx->root, ctx->foreignrel, linitial(expr->args), ctx->params_list);
  if (arg != NULL) {
    bool bBreak = false;
    appendStringInfo (&buf, "(%s%s", expr->boolop == NOT_EXPR ? "NOT " : "", arg);
    do_each_cell(cell, expr->args, list_next(expr->args, list_head(expr->args))) { 
      db2free(arg);
      arg = deparseExpr (ctx->root, ctx->foreignrel, (Expr*)lfirst(cell), ctx->params_list);
      if (arg != NULL) {
        appendStringInfo (&buf, " %s %s", expr->boolop == AND_EXPR ? "AND":"OR", arg);
      } else {
        bBreak = true;
        break;
      }
    }
    if (!bBreak) {
      appendStringInfo (ctx->buf, "%s)", buf.data);
    }
  }
  db2free(buf.data);
  db2free(arg);
  db2Debug1("< %s::deparseBoolExpr: %s", __FILE__, ctx->buf->data);
}

static void deparseCaseExpr          (CaseExpr*          expr, deparse_expr_cxt* ctx) {
  db2Debug1("> %s::deparseCaseExpr", __FILE__);
  if (!canHandleType (expr->casetype)) {
    db2Debug2("  cannot Handle Type caseexpr->casetype (%d)", expr->casetype);
  } else {
    StringInfoData buf;
    bool           bBreak    = false;
    char*          arg       = NULL;
    ListCell*      cell      = NULL;

    initStringInfo   (&buf);
    appendStringInfo (&buf, "CASE");

    if (expr->arg != NULL) {
      /* for the form "CASE arg WHEN ...", add first expression */
      arg = deparseExpr (ctx->root, ctx->foreignrel, expr->arg, ctx->params_list);
      db2Debug2("  CASE %s WHEN ...", arg);
      if (arg == NULL) {
        appendStringInfo (&buf, " %s", arg);
      } else {
        bBreak = true;
      }
    }
    if (!bBreak) {
      /* append WHEN ... THEN clauses */
      foreach (cell, expr->args) {
        CaseWhen* whenclause = (CaseWhen*) lfirst (cell);
        /* WHEN */
        if (expr->arg == NULL) {
          /* for CASE WHEN ..., use the whole expression */
          arg = deparseExpr (ctx->root, ctx->foreignrel, whenclause->expr, ctx->params_list);
        } else {
          /* for CASE arg WHEN ..., use only the right branch of the equality */
          arg = deparseExpr (ctx->root, ctx->foreignrel, lsecond (((OpExpr*) whenclause->expr)->args), ctx->params_list);
        }
        db2Debug2(" WHEN %s ", arg);
        if (arg != NULL) {
          appendStringInfo (&buf, " WHEN %s", arg);
        } else {
          bBreak = true;
          break;
        }    /* THEN */
        arg = deparseExpr (ctx->root, ctx->foreignrel, whenclause->result, ctx->params_list);
        db2Debug2(" THEN %s ", arg);
        if (arg != NULL) {
          appendStringInfo (&buf, " THEN %s", arg);
        } else {
          bBreak = true;
          break;
        }
      }
      if (!bBreak) {
        /* append ELSE clause if appropriate */
        if (expr->defresult != NULL) {
          arg = deparseExpr (ctx->root, ctx->foreignrel, expr->defresult, ctx->params_list);
          db2Debug2("  ELSE %s", arg);
          if (arg != NULL) {
            appendStringInfo (&buf, " ELSE %s", arg);
          } else {
            bBreak = true;
          }
        }
        /* append END */
        appendStringInfo (&buf, " END");
      }
    }
    if (!bBreak) {
      appendStringInfo(ctx->buf,"%s",buf.data);
    }
    db2free(buf.data);
  }
  db2Debug1("< %s::deparseCaseExpr: %s", __FILE__, ctx->buf->data);
}

static void deparseCoalesceExpr      (CoalesceExpr*      expr, deparse_expr_cxt* ctx) {
  db2Debug1("> %s::deparseCoalesceExpr", __FILE__);
  if (!canHandleType (expr->coalescetype)) {
    db2Debug2("  cannot Handle Type coalesceexpr->coalescetype (%d)", expr->coalescetype);
  } else {
    StringInfoData result;
    char*          arg       = NULL;
    bool           first_arg = true;
    ListCell*      cell      = NULL;

    initStringInfo   (&result);
    appendStringInfo (&result, "COALESCE(");
    foreach (cell, expr->args) {
      arg = deparseExpr (ctx->root, ctx->foreignrel, (Expr*)lfirst(cell),ctx->params_list);
      db2Debug2("  arg: %s", arg);
      if (arg != NULL) {
        appendStringInfo(&result, ((first_arg) ? "%s" : ", %s"), arg);
        first_arg = false;
      } else {
        break;
      }
    }
    if (arg != NULL) {
      appendStringInfo (ctx->buf, "%s)",result.data);
    }
    db2free(result.data);
  }
  db2Debug1("< %s::deparseCoalesceExpr: %s", __FILE__, ctx->buf->data);
}

static void deparseFuncExpr          (FuncExpr*          expr, deparse_expr_cxt* ctx) {
  db2Debug1("> %s::deparseFuncExpr", __FILE__);
  if (!canHandleType (expr->funcresulttype)) {
    db2Debug2(" cannot handle funct->funcresulttype: %d",expr->funcresulttype);
  } else if (expr->funcformat == COERCE_IMPLICIT_CAST) {
      /* do nothing for implicit casts */
      db2Debug2(" COERCE_IMPLICIT_CAST == expr->funcformat(%d)",expr->funcformat);
      deparseExprInt (linitial(expr->args), ctx);
  } else {
    Oid       schema;
    char*     opername;
    HeapTuple tuple;
    
    /* get function name and schema */
    tuple = SearchSysCache1 (PROCOID, ObjectIdGetDatum (expr->funcid));
    if (!HeapTupleIsValid (tuple)) {
      elog (ERROR, "cache lookup failed for function %u", expr->funcid);
    }
    opername = db2strdup (((Form_pg_proc) GETSTRUCT (tuple))->proname.data);
    db2Debug2("  opername: %s",opername);
    schema = ((Form_pg_proc) GETSTRUCT (tuple))->pronamespace;
    db2Debug2("  schema: %d",schema);
    ReleaseSysCache (tuple);
    /* ignore functions in other than the pg_catalog schema */
    if (schema != PG_CATALOG_NAMESPACE) {
      db2Debug2("  T_FuncExpr: schema(%d) != PG_CATALOG_NAMESPACE", schema);
    } else {
      /* the "normal" functions that we can translate */
      if (strcmp (opername, "abs")          == 0 || strcmp (opername, "acos")         == 0 || strcmp (opername, "asin")             == 0
      ||  strcmp (opername, "atan")         == 0 || strcmp (opername, "atan2")        == 0 || strcmp (opername, "ceil")             == 0
      ||  strcmp (opername, "ceiling")      == 0 || strcmp (opername, "char_length")  == 0 || strcmp (opername, "character_length") == 0
      ||  strcmp (opername, "concat")       == 0 || strcmp (opername, "cos")          == 0 || strcmp (opername, "exp")              == 0
      ||  strcmp (opername, "initcap")      == 0 || strcmp (opername, "length")       == 0 || strcmp (opername, "lower")            == 0
      ||  strcmp (opername, "lpad")         == 0 || strcmp (opername, "ltrim")        == 0 || strcmp (opername, "mod")              == 0
      ||  strcmp (opername, "octet_length") == 0 || strcmp (opername, "position")     == 0 || strcmp (opername, "pow")              == 0
      ||  strcmp (opername, "power")        == 0 || strcmp (opername, "replace")      == 0 || strcmp (opername, "round")            == 0
      ||  strcmp (opername, "rpad")         == 0 || strcmp (opername, "rtrim")        == 0 || strcmp (opername, "sign")             == 0
      ||  strcmp (opername, "sin")          == 0 || strcmp (opername, "sqrt")         == 0 || strcmp (opername, "strpos")           == 0
      ||  strcmp (opername, "substr")       == 0 || strcmp (opername, "tan")          == 0 || strcmp (opername, "to_char")          == 0
      ||  strcmp (opername, "to_date")      == 0 || strcmp (opername, "to_number")    == 0 || strcmp (opername, "to_timestamp")     == 0
      ||  strcmp (opername, "translate")    == 0 || strcmp (opername, "trunc")        == 0 || strcmp (opername, "upper")            == 0
      || (strcmp (opername, "substring")    == 0 && list_length (expr->args) == 3)) {
        ListCell*      cell;
        char*          arg       = NULL;
        bool           ok        = true;
        bool           first_arg = true;
        StringInfoData buf;

        initStringInfo (&buf);
        if (strcmp (opername, "ceiling") == 0)
          appendStringInfo (&buf, "CEIL(");
        else if (strcmp (opername, "char_length") == 0 || strcmp (opername, "character_length") == 0)
          appendStringInfo (&buf, "LENGTH(");
        else if (strcmp (opername, "pow") == 0)
          appendStringInfo (&buf, "POWER(");
        else if (strcmp (opername, "octet_length") == 0)
          appendStringInfo (&buf, "LENGTHB(");
        else if (strcmp (opername, "position") == 0 || strcmp (opername, "strpos") == 0)
          appendStringInfo (&buf, "INSTR(");
        else if (strcmp (opername, "substring") == 0)
          appendStringInfo (&buf, "SUBSTR(");
        else
          appendStringInfo (&buf, "%s(", opername);
        foreach (cell, expr->args) {
          arg = deparseExpr (ctx->root, ctx->foreignrel, lfirst (cell), ctx->params_list);
          if (arg != NULL) {
            appendStringInfo (&buf, "%s%s", (first_arg) ? ", " : "",arg);
            first_arg = false;
            db2free(arg);
          } else {
            ok = false;
            db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
            break;
          }
        }
        appendStringInfo (&buf, ")");
        // copy to return value when successful
        if (ok) {
          appendStringInfo(ctx->buf,"%s",buf.data);
        }
        db2free(buf.data);
      } else if (strcmp (opername, "date_part") == 0) {
        char* left = NULL;

        /* special case: EXTRACT */
        left = deparseExpr (ctx->root, ctx->foreignrel, linitial (expr->args), ctx->params_list);
        if (left == NULL) {
          db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
        } else {
          /* can only handle these fields in DB2 */
          if (strcmp (left, "'year'")          == 0 || strcmp (left, "'month'")           == 0
          ||  strcmp (left, "'day'")           == 0 || strcmp (left, "'hour'")            == 0
          ||  strcmp (left, "'minute'")        == 0 || strcmp (left, "'second'")          == 0
          ||  strcmp (left, "'timezone_hour'") == 0 || strcmp (left, "'timezone_minute'") == 0) {
            char* right = NULL;

            /* remove final quote */
            left[strlen (left) - 1] = '\0';
            right = deparseExpr (ctx->root, ctx->foreignrel, lsecond (expr->args), ctx->params_list);
            if (right == NULL) {
              db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
            } else {
              appendStringInfo (ctx->buf, "EXTRACT(%s FROM %s)", left + 1, right);
            }
            db2free(right);
          } else {
            db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
          }
        }
        db2free (left);
      } else if (strcmp (opername, "now") == 0 || strcmp (opername, "transaction_timestamp") == 0) {
        /* special case: current timestamp */
        appendStringInfo (ctx->buf, "(CAST (?/*:now*/ AS TIMESTAMP))");
      } else {
        /* function that we cannot render for DB2 */
        db2Debug2("  T_FuncExpr: function %s that we cannot render for DB2", opername);
      }
    }
    db2free (opername);
  }
  db2Debug1("< %s::deparseFuncExpr: %s", __FILE__, ctx->buf->data);
}

static void deparseCoerceViaIOExpr   (CoerceViaIO*       expr, deparse_expr_cxt* ctx) {
  db2Debug1("> %s::deparseCoerceViaIOExpr", __FILE__);
  /* We will only handle casts of 'now'.   */
  /* only casts to these types are handled */
  if (expr->resulttype != DATEOID && expr->resulttype != TIMESTAMPOID && expr->resulttype != TIMESTAMPTZOID) {
    db2Debug2("  only casts to DATEOID, TIMESTAMPOID and TIMESTAMPTZOID are handled");
  } else if (expr->arg->type != T_Const) {
  /* the argument must be a Const */
    db2Debug2("  T_CoerceViaIO: the argument must be a Const");
  } else {
    Const* constant = (Const *) expr->arg;
    if (constant->constisnull || (constant->consttype != CSTRINGOID && constant->consttype != TEXTOID)) {
    /* the argument must be a not-NULL text constant */
      db2Debug2("  T_CoerceViaIO: the argument must be a not-NULL text constant");
    } else {
      /* get the type's output function */
      HeapTuple tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (constant->consttype));
      regproc   typoutput;
      if (!HeapTupleIsValid (tuple)) {
        elog (ERROR, "cache lookup failed for type %u", constant->consttype);
      }
      typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
      ReleaseSysCache (tuple);
      /* the value must be "now" */
      if (strcmp (DatumGetCString (OidFunctionCall1 (typoutput, constant->constvalue)), "now") != 0) {
        db2Debug2("  value must be 'now'");
      } else {
        switch (expr->resulttype) {
          case DATEOID:
            appendStringInfo(ctx->buf, "TRUNC(CAST (CAST(?/*:now*/ AS TIMESTAMP) AS DATE))");
          break;
          case TIMESTAMPOID:
            appendStringInfo(ctx->buf, "(CAST (CAST (?/*:now*/ AS TIMESTAMP) AS TIMESTAMP))");
          break;
          case TIMESTAMPTZOID:
            appendStringInfo(ctx->buf, "(CAST (?/*:now*/ AS TIMESTAMP))");
          break;
          case TIMEOID:
            appendStringInfo(ctx->buf, "(CAST (CAST (?/*:now*/ AS TIME) AS TIME))");
          break;
          case TIMETZOID:
            appendStringInfo(ctx->buf, "(CAST (?/*:now*/ AS TIME))");
          break;
        }
      }
    }
  }
  db2Debug1("< %s::deparseCoerceViaIOExpr: %s", __FILE__, ctx->buf->data);
}

static void deparseSQLValueFuncExpr  (SQLValueFunction*  expr, deparse_expr_cxt* ctx) {
  db2Debug1("> %s::deparseSQLValueFuncExpr", __FILE__);
  switch (expr->op) {
    case SVFOP_CURRENT_DATE:
      appendStringInfo(ctx->buf, "TRUNC(CAST (CAST(?/*:now*/ AS TIMESTAMP) AS DATE))");
    break;
    case SVFOP_CURRENT_TIMESTAMP:
      appendStringInfo(ctx->buf, "(CAST (?/*:now*/ AS TIMESTAMP))");
    break;
    case SVFOP_LOCALTIMESTAMP:
      appendStringInfo(ctx->buf, "(CAST (CAST (?/*:now*/ AS TIMESTAMP) AS TIMESTAMP))");
    break;
    case SVFOP_CURRENT_TIME:
      appendStringInfo(ctx->buf, "(CAST (?/*:now*/ AS TIME))");
    break;
    case SVFOP_LOCALTIME:
      appendStringInfo(ctx->buf, "(CAST (CAST (?/*:now*/ AS TIME) AS TIME))");
    break;
    default:
      /* don't push down other functions */
      db2Debug2("  op %d cannot be translated to DB2", expr->op);
    break;
  }
  db2Debug1("< %s::deparseSQLValueFuncExpr: %s", __FILE__, ctx->buf->data);
}

static void deparseAggref            (Aggref*            expr, deparse_expr_cxt* ctx) {
  db2Debug1("> %s::deparseAggref", __FILE__);
  if (expr == NULL) {
    db2Debug2("  expr is NULL");
  } else {
    /* Resolve aggregate function name (OID -> pg_proc.proname). */
    HeapTuple tuple   = SearchSysCache1(PROCOID, ObjectIdGetDatum(expr->aggfnoid));
    char*     aggname = NULL;
    char*     nspname = NULL;
    if (!HeapTupleIsValid(tuple)) {
      elog(ERROR, "cache lookup failed for function %u", expr->aggfnoid);
    } else {
      Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(tuple);
      aggname = pstrdup(NameStr(procform->proname));
      /* Optional: capture schema for debugging/qualification decisions. */
      if (OidIsValid(procform->pronamespace)) {
        HeapTuple ntup = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(procform->pronamespace));
        if (HeapTupleIsValid(ntup)) {
          Form_pg_namespace nspform = (Form_pg_namespace) GETSTRUCT(ntup);
          nspname = pstrdup(NameStr(nspform->nspname));
          ReleaseSysCache(ntup);
        }
      }
      ReleaseSysCache(tuple);
    }
    db2Debug2( "  aggref->aggfnoid=%u name=%s%s%s"
             , expr->aggfnoid
             , nspname ? nspname : ""
             , nspname ? "." : ""
             , aggname ? aggname : "<unknown>"
             );
    /* We only support deparsing simple, standard aggregates for now.
     * (This can be expanded to ordered-set / FILTER / WITHIN GROUP later.)
     */
    if (expr->aggorder != NIL) {
      db2Debug2("  aggregate ORDER BY not supported for pushdown");
    } else if (aggname != NULL) {
      const char*    db2func  = NULL;
      bool           distinct = (expr->aggdistinct != NIL);
      bool           ok = true;
   
      if (strcmp(aggname, "count") == 0) db2func = "COUNT";
      else if (strcmp(aggname, "sum") == 0) db2func = "SUM";
      else if (strcmp(aggname, "avg") == 0) db2func = "AVG";
      else if (strcmp(aggname, "min") == 0) db2func = "MIN";
      else if (strcmp(aggname, "max") == 0) db2func = "MAX";
      else {
        /* Unknown aggregate name: we can still report it (above), but don't emit SQL. */
        db2Debug2("  aggregate '%s' not supported for DB2 deparse", aggname);
      } 
      if (db2func != NULL) {
        StringInfoData    result;

        initStringInfo(&result);
        appendStringInfo(&result, "%s(", db2func);
        if (distinct) {
          appendStringInfoString(&result, "DISTINCT ");
        }
        if (expr->aggstar) {
          /* COUNT(*) */
          appendStringInfoString(&result, "*");
        } else {
          ListCell* lc;
          char*     arg  = NULL;
          bool      first_arg = true;

          foreach (lc, expr->args) {
            Node* argnode = (Node*) lfirst(lc);
            Expr* argexpr = NULL;

            if (argnode == NULL) {
              ok = false;
              break;
            }
            if (argnode->type == T_TargetEntry) {
              argexpr = ((TargetEntry*) argnode)->expr;
            } else {
              argexpr = (Expr*) argnode;
            }
            arg = deparseExpr(ctx->root, ctx->foreignrel, argexpr, ctx->params_list);
            if (arg == NULL) {
              ok = false;
              break;
            }
            appendStringInfo(&result, "%s%s", arg, first_arg ? "" : ", ");
            first_arg = false;
          }
        }
        if (ok) {
          appendStringInfo(ctx->buf, "%s)",result.data);
        } else {
          db2Debug2("  parsed aggref so far: %s", result.data);
          db2Debug2("  could not deparse aggregate args");
        }
        db2free(result.data);
      }
    }
  }
  db2Debug1("< %s::deparseAggref: %s", __FILE__, ctx->buf->data);
}

/** datumToString
 *   Convert a Datum to a string by calling the type output function.
 *   Returns the result or NULL if it cannot be converted to DB2 SQL.
 */
static char* datumToString (Datum datum, Oid type) {
  StringInfoData result;
  regproc        typoutput;
  HeapTuple      tuple;
  char*          str;
  char*          p;
  db2Debug1("> %s::datumToString", __FILE__);
  /* get the type's output function */
  tuple = SearchSysCache1 (TYPEOID, ObjectIdGetDatum (type));
  if (!HeapTupleIsValid (tuple)) {
    elog (ERROR, "cache lookup failed for type %u", type);
  }
  typoutput = ((Form_pg_type) GETSTRUCT (tuple))->typoutput;
  ReleaseSysCache (tuple);

  /* render the constant in DB2 SQL */
  switch (type) {
    case TEXTOID:
    case CHAROID:
    case BPCHAROID:
    case VARCHAROID:
    case NAMEOID:
      str = DatumGetCString (OidFunctionCall1 (typoutput, datum));
      /*
       * Don't try to convert empty strings to DB2.
       * DB2 treats empty strings as NULL.
       */
      if (str[0] == '\0')
        return NULL;

      /* quote string */
      initStringInfo (&result);
      appendStringInfo (&result, "'");
      for (p = str; *p; ++p) {
        if (*p == '\'')
          appendStringInfo (&result, "'");
        appendStringInfo (&result, "%c", *p);
      }
      appendStringInfo (&result, "'");
    break;
    case INT8OID:
    case INT2OID:
    case INT4OID:
    case OIDOID:
    case FLOAT4OID:
    case FLOAT8OID:
    case NUMERICOID:
      str = DatumGetCString (OidFunctionCall1 (typoutput, datum));
      initStringInfo (&result);
      appendStringInfo (&result, "%s", str);
    break;
    case DATEOID:
      str = deparseDate (datum);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS DATE))", str);
    break;
    case TIMESTAMPOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIMESTAMP))", str);
    break;
    case TIMESTAMPTZOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIMESTAMP))", str);
    break;
    case TIMEOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIME))", str);
    break;
    case TIMETZOID:
      str = deparseTimestamp (datum, false);
      initStringInfo (&result);
      appendStringInfo (&result, "(CAST ('%s' AS TIME))", str);
    break;
    case INTERVALOID:
      str = deparseInterval (datum);
      if (str == NULL)
        return NULL;
      initStringInfo (&result);
      appendStringInfo (&result, "%s", str);
    break;
    default:
      return NULL;
  }
  db2Debug1("< %s::datumToString - returns: '%s'", __FILE__, result.data);
  return result.data;
}

/** deparseDate
 *   Render a PostgreSQL date so that DB2 can parse it.
 */
char* deparseDate (Datum datum) {
  struct pg_tm   datetime_tm;
  StringInfoData s;
  db2Debug1("> %s::deparseDate", __FILE__);
  if (DATE_NOT_FINITE (DatumGetDateADT (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite date value cannot be stored in DB2")));

  /* get the parts */
  (void) j2date (DatumGetDateADT (datum) + POSTGRES_EPOCH_JDATE, &(datetime_tm.tm_year), &(datetime_tm.tm_mon), &(datetime_tm.tm_mday));

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  appendStringInfo (&s, "%04d-%02d-%02d 00:00:00", datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1, datetime_tm.tm_mon, datetime_tm.tm_mday);
  db2Debug1("< %s::deparseDate - returns: '%s'", __FILE__, s.data);
  return s.data;
}

/** deparseTimestamp
 *   Render a PostgreSQL timestamp so that DB2 can parse it.
 */
char* deparseTimestamp (Datum datum, bool hasTimezone) {
  struct pg_tm   datetime_tm;
  int32          tzoffset;
  fsec_t         datetime_fsec;
  StringInfoData s;
  db2Debug1("> %s::deparseTimestamp",__FILE__);
  /* this is sloppy, but DatumGetTimestampTz and DatumGetTimestamp are the same */
  if (TIMESTAMP_NOT_FINITE (DatumGetTimestampTz (datum)))
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("infinite timestamp value cannot be stored in DB2")));

  /* get the parts */
  tzoffset = 0;
  (void) timestamp2tm (DatumGetTimestampTz (datum), hasTimezone ? &tzoffset : NULL, &datetime_tm, &datetime_fsec, NULL, NULL);

  if (datetime_tm.tm_year < 0)
    ereport (ERROR, (errcode (ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE), errmsg ("BC date value cannot be stored in DB2")));

  initStringInfo (&s);
  if (hasTimezone)
    appendStringInfo (&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d%+03d:%02d",
      datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
      datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
      datetime_tm.tm_min, datetime_tm.tm_sec, (int32) datetime_fsec,
      -tzoffset / 3600, ((tzoffset > 0) ? tzoffset % 3600 : -tzoffset % 3600) / 60);
  else
    appendStringInfo (&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
      datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
      datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
      datetime_tm.tm_min, datetime_tm.tm_sec, (int32) datetime_fsec);
  db2Debug1("< %s::deparseTimestamp - returns: '%s'", __FILE__, s.data);
  return s.data;
}

/** deparsedeparseInterval
 *   Render a PostgreSQL timestamp so that DB2 can parse it.
 */
static char* deparseInterval (Datum datum) {
  #if PG_VERSION_NUM >= 150000
  struct pg_itm tm;
  #else
  struct pg_tm tm;
  #endif
  fsec_t         fsec=0;
  StringInfoData s;
  char*          sign;
  int            idx = 0;

  db2Debug1("> %s::deparseInterval",__FILE__);
  #if PG_VERSION_NUM >= 150000
  interval2itm (*DatumGetIntervalP (datum), &tm);
  #else
  if (interval2tm (*DatumGetIntervalP (datum), &tm, &fsec) != 0) {
    elog (ERROR, "could not convert interval to tm");
  }
  #endif
  /* only translate intervals that can be translated to INTERVAL DAY TO SECOND */
//  if (tm.tm_year != 0 || tm.tm_mon != 0)
//    return NULL;

  /* DB2 intervals have only one sign */
  if (tm.tm_mday < 0 || tm.tm_hour < 0 || tm.tm_min < 0 || tm.tm_sec < 0 || fsec < 0) {
    sign = "-";
    /* all signs must match */
    if (tm.tm_mday > 0 || tm.tm_hour > 0 || tm.tm_min > 0 || tm.tm_sec > 0 || fsec > 0)
      return NULL;
    tm.tm_mday = -tm.tm_mday;
    tm.tm_hour = -tm.tm_hour;
    tm.tm_min  = -tm.tm_min;
    tm.tm_sec  = -tm.tm_sec;
    fsec       = -fsec;
  } else {
    sign = "+";
  }
  initStringInfo (&s);
  if (tm.tm_year > 0) {
    appendStringInfo(&s, ((tm.tm_year > 1) ? "%d YEARS" : "%d YEAR"),tm.tm_year);
  }
  idx += tm.tm_year;
  if (tm.tm_mon > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_mon > 1) ? "%d MONTHS" : "%d MONTH"),tm.tm_mon);
  }
  idx += tm.tm_mon;
  if (tm.tm_mday > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_mday > 1) ? "%d DAYS" : "%d DAY"),tm.tm_mday);
  }
  idx += tm.tm_mday;
  if (tm.tm_hour > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    #if PG_VERSION_NUM >= 150000
    appendStringInfo(&s, ((tm.tm_hour > 1) ? "%ld HOURS" : "%ld HOUR"),tm.tm_hour);
    #else
    appendStringInfo(&s, ((tm.tm_hour > 1) ? "%d HOURS" : "%d HOUR"),tm.tm_hour);
    #endif
  }
  idx += tm.tm_hour;
  if (tm.tm_min > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_min > 1) ? "%d MINUTES" : "%d MINUTE"),tm.tm_min);
  }
  idx += tm.tm_min;
  if (tm.tm_sec > 0) {
    appendStringInfo(&s," %s ",(idx > 0 ) ? sign : "");
    appendStringInfo(&s, ((tm.tm_sec > 1) ? "%d SECONDS" : "%d SECOND"),tm.tm_sec);
  }
  idx += tm.tm_sec;

//  #if PG_VERSION_NUM >= 150000
//  appendStringInfo (&s, "INTERVAL '%s%d %02ld:%02d:%02d.%06d' DAY TO SECOND", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
//  #else
//  appendStringInfo (&s, "INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY(9) TO SECOND(6)", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);
//  #endif
  db2Debug1("< %s::deparseInterval - returns: '%s'",__FILE__,s.data);
  return s.data;
}

/** Output join name for given join type 
 */
static const char* get_jointype_name (JoinType jointype) {
  char* type = NULL;
  db2Debug1("> get_jointype_name");
  switch (jointype) {
    case JOIN_INNER:
      type = "INNER";
    break;
    case JOIN_LEFT:
      type = "LEFT";
    break;
    case JOIN_RIGHT:
      type = "RIGHT";
    break;
    case JOIN_FULL:
      type= "FULL";
    break;
    default:
      /* Shouldn't come here, but protect from buggy code. */
      elog (ERROR, "unsupported join type %d", jointype);
    break;
  }
  db2Debug2("  type: '%s'",type);
  db2Debug1("< get_jointype_name");
  return type;
}
