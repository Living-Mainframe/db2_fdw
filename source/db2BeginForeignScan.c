#include <postgres.h>
#include <commands/explain.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include <access/xact.h>
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern DB2Session*  db2GetSession             (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern void*        db2alloc                  (const char* type, size_t size);
extern DB2FdwState* deserializePlanData       (List* list);
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);

/** local prototypes */
        void  db2BeginForeignScan (ForeignScanState* node, int eflags);
static  int   addExprParams       (ForeignScanState* node);
static  int   addTleExprParams    (ForeignScanState* node, int paramCount);

/** db2BeginForeignScan
 *   Recover ("deserialize") connection information, remote query,
 *   DB2 table description and parameter list from the plan's
 *   "fdw_private" field.
 *   Reestablish a connection to DB2.
 */
void db2BeginForeignScan(ForeignScanState* node, int eflags) {
  ForeignScan* fsplan      = (ForeignScan*) node->ss.ps.plan;
  DB2FdwState* fdw_state   = NULL;
  int          paramCount  = 0;

  db2Debug1("> db2BeginForeignScan");
  /* deserialize private plan data */
  fdw_state       = deserializePlanData(fsplan->fdw_private);
  node->fdw_state = (void *) fdw_state;

  paramCount  = addExprParams(node);
  addTleExprParams(node,paramCount);

  /* add a fake parameter "if that string appears in the query */
  if (strstr (fdw_state->query, "?/*:now*/") != NULL) {
    ParamDesc*  paramDesc = (ParamDesc*) db2alloc ("fdw_state->paramList->next", sizeof (ParamDesc));
    paramDesc->type      = TIMESTAMPTZOID;
    paramDesc->bindType  = BIND_STRING;
    paramDesc->value     = NULL;
    paramDesc->node      = NULL;
    paramDesc->colnum    = -1;
    paramDesc->txts      = 1;
    paramDesc->next      = fdw_state->paramList;
    fdw_state->paramList = paramDesc;
  }

  if (node->ss.ss_currentRelation)
    elog (DEBUG3, "  begin foreign table scan on relid: %d", RelationGetRelid (node->ss.ss_currentRelation));
  else
    elog (DEBUG3, "  begin foreign join");

  /* connect to DB2 database */
  fdw_state->session = db2GetSession (fdw_state->dbserver
                                     ,fdw_state->user
                                     ,fdw_state->password
                                     ,fdw_state->jwt_token
                                     ,fdw_state->nls_lang
                                     ,GetCurrentTransactionNestLevel()
    );

  /* initialize row count to zero */
  fdw_state->rowcount = 0;
  db2Debug1("< db2BeginForeignScan");
}

static int addExprParams(ForeignScanState* node){
  DB2FdwState* fdw_state   = node->fdw_state;
  ForeignScan* fsplan      = (ForeignScan*) node->ss.ps.plan;
  List*        exec_exprs  = NIL;
  ParamDesc*   paramDesc   = NULL;
  ListCell*    cell        = NULL;
  int          index       = 0;

  db2Debug1("> addExprParams");
  /* create an ExprState tree for the parameter expressions */
  exec_exprs = (List*) ExecInitExprList (fsplan->fdw_exprs, (PlanState*) node);
  db2Debug2("  exec_expr: %x[%d]",exec_exprs, list_length(exec_exprs));
  /* create the list of parameters */
  foreach (cell, exec_exprs) {
    ExprState* expr = (ExprState*) lfirst (cell);

    /* count, but skip deleted entries */
    ++index;
    if (expr == NULL)
      continue;

    /* create a new entry in the parameter list */
    paramDesc       = (ParamDesc*) db2alloc("fdw_state->paramList->next", sizeof (ParamDesc));
    paramDesc->type = exprType ((Node*) (expr->expr));

    if (paramDesc->type == TEXTOID
    ||  paramDesc->type == VARCHAROID
    ||  paramDesc->type == BPCHAROID
    ||  paramDesc->type == CHAROID
    ||  paramDesc->type == DATEOID
    ||  paramDesc->type == TIMESTAMPOID
    ||  paramDesc->type == TIMESTAMPTZOID
    ||  paramDesc->type == TIMEOID
    ||  paramDesc->type == TIMETZOID)
      paramDesc->bindType = BIND_STRING;
    else
      paramDesc->bindType = BIND_NUMBER;

    paramDesc->value     = NULL;
    paramDesc->node      = expr;
    paramDesc->colnum    = -1;
    paramDesc->txts      = 0;
    paramDesc->next      = fdw_state->paramList;
    db2Debug2("  paramDesc->colnum: %d  ",paramDesc->colnum);
    fdw_state->paramList = paramDesc;
  }
  db2Debug1("< addExprParams : %d", index);
  return index;
}

static int addTleExprParams(ForeignScanState* node, int paramCount){
  DB2FdwState* fdw_state   = node->fdw_state;
  ForeignScan* fsplan      = (ForeignScan*) node->ss.ps.plan;
  List*        tle_exprs   = fsplan->fdw_scan_tlist;
  ParamDesc*   paramDesc   = NULL;
  ListCell*    cell        = NULL;
  int          index       = 0;

  db2Debug1("> addTleExprParams");
  db2Debug2("  tle_expr: %x[%d]",tle_exprs, list_length(tle_exprs));

  // create the list of parameters
  foreach (cell, tle_exprs) {
    Expr*         expr  = NULL;
    TargetEntry*  tle   = (TargetEntry*) lfirst(cell);

    if (tle == NULL)
      continue;

    // count, but skip deleted entries
    db2Debug2("  result index: %d",++index);
    db2Debug2("  tle->resno: %d", tle->resno);
    expr =  (Expr*) tle->expr;
    if (expr != NULL ) {
      db2Debug2("  tle->expr->type: %d", expr->type);
/*
      switch(tle->expr->type) {
        case T_Aggref: {
          Aggref* agg = (Aggref*) expr;
          db2Debug2("  agg->aggno   : %d", agg->aggno);
          db2Debug2("  agg->aggstar : %s", agg->aggstar ? "true" : "false");
          db2Debug2("  agg->aggtype : %d", agg->aggtype);
          paramDesc            = (ParamDesc*) db2alloc("fdw_state->paramList->next", sizeof (ParamDesc));
          paramDesc->type      = agg->aggtype;
          paramDesc->bindType  = (agg->aggtype == NUMERICOID) ? BIND_NUMBER : BIND_STRING;
          paramDesc->value     = NULL;
          paramDesc->node      = agg;
          paramDesc->colnum    = -1;
          paramDesc->txts      = 0;
          paramDesc->next      = fdw_state->paramList;
          db2Debug2("  paramDesc->colnum: %d  ",paramDesc->colnum);
          fdw_state->paramList = paramDesc;
        }
        break;
        default:
          continue;  // skip non-constant expressions
        break;
      }
*/
    }
/*
    // create a new entry in the parameter list
    paramDesc       = (ParamDesc*) db2alloc("fdw_state->paramList->next", sizeof (ParamDesc));
    paramDesc->type = exprType ((Node*)(expr->expr));

    if (paramDesc->type == TEXTOID
    ||  paramDesc->type == VARCHAROID
    ||  paramDesc->type == BPCHAROID
    ||  paramDesc->type == CHAROID
    ||  paramDesc->type == DATEOID
    ||  paramDesc->type == TIMESTAMPOID
    ||  paramDesc->type == TIMESTAMPTZOID
    ||  paramDesc->type == TIMEOID
    ||  paramDesc->type == TIMETZOID)
      paramDesc->bindType = BIND_STRING;
    else
      paramDesc->bindType = BIND_NUMBER;

    paramDesc->value     = NULL;
    paramDesc->node      = expr;
    paramDesc->colnum    = -1;
    paramDesc->txts      = 0;
    paramDesc->next      = fdw_state->paramList;
    db2Debug2("  paramDesc->colnum: %d  ",paramDesc->colnum);
    fdw_state->paramList = paramDesc;
*/
  }

  db2Debug1("< addTleExprParams : %d", index);
  return index;
}