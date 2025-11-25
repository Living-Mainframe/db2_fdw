#include <postgres.h>
#include <commands/explain.h>
#include <nodes/nodeFuncs.h>
#if PG_VERSION_NUM < 120000
#include <nodes/relation.h>
#include <optimizer/var.h>
#include <utils/tqual.h>
#else
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include <access/xact.h>
#endif
#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern DB2Session*  db2GetSession             (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern void*        db2alloc                  (const char* type, size_t size);
extern DB2FdwState* deserializePlanData       (List* list);
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug2                 (const char* message, ...);

/** local prototypes */
void db2BeginForeignScan(ForeignScanState* node, int eflags);

/** db2BeginForeignScan
 *   Recover ("deserialize") connection information, remote query,
 *   DB2 table description and parameter list from the plan's
 *   "fdw_private" field.
 *   Reestablish a connection to DB2.
 */
void db2BeginForeignScan(ForeignScanState* node, int eflags) {
  ForeignScan* fsplan      = (ForeignScan*) node->ss.ps.plan;
  List*        fdw_private = fsplan->fdw_private;
  List*        exec_exprs  = NULL;
  ListCell*    cell        = NULL;
  int          index       = 0;
  ParamDesc*   paramDesc   = NULL;
  DB2FdwState* fdw_state   = NULL;

  db2Debug1("> db2BeginForeignScan");
  /* deserialize private plan data */
  fdw_state       = deserializePlanData(fdw_private);
  node->fdw_state = (void *) fdw_state;

  /* create an ExprState tree for the parameter expressions */
#if PG_VERSION_NUM < 100000
  exec_exprs = (List *) ExecInitExpr ((Expr *) fsplan->fdw_exprs, (PlanState *) node);
#else
  exec_exprs = (List *) ExecInitExprList (fsplan->fdw_exprs, (PlanState *) node);
#endif /* PG_VERSION_NUM */

  /* create the list of parameters */
  index = 0;
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

  /* add a fake parameter "if that string appears in the query */
  if (strstr (fdw_state->query, "?/*:now*/") != NULL) {
    paramDesc            = (ParamDesc*) db2alloc ("fdw_state->paramList->next", sizeof (ParamDesc));
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
