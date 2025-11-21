#include <postgres.h>
#include <nodes/makefuncs.h>
#if PG_VERSION_NUM < 120000
#include <nodes/relation.h>
#include <optimizer/var.h>
#include <utils/tqual.h>
#else
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#endif

#include "db2_fdw.h"
#include "DB2FdwState.h"
#include "ParamDesc.h"
#include "DB2Table.h"
#include "DB2Column.h"

/** external prototypes */
extern void            db2free                   (void* p);
extern void            db2Debug1                 (const char* message, ...);

/** local prototypes */
void db2CleanupFdwState(DB2FdwState* fdw_state);

void db2CleanupFdwState(DB2FdwState* fdw_state) {
  ParamDesc*  current  = NULL;
  ParamDesc*  tofree   = NULL;
  DB2Table*   db2Table = NULL;
  DB2Column** cols     = NULL;
  int         i        = 0;


  db2Debug1("> db2CleanupFdwState");
  db2free(fdw_state->dbserver);
  db2free(fdw_state->user);
  db2free(fdw_state->jwt_token);
  db2free(fdw_state->nls_lang);
  // reslease anything allocated in ParamDesc
  current = fdw_state->paramList;
  tofree  = NULL;
  while (current != NULL) {
    db2free(current->value);
    db2free(current->node);
    tofree  = current;
    current = current->next;
    db2free(tofree);
  }
  db2free(fdw_state->query);
  db2Table = fdw_state->db2Table;
  db2free(db2Table->name);
  db2free(db2Table->pgname);
  cols = db2Table->cols;
  // iterate thru all columns and free stuff
  for (i = 0; i < db2Table->ncols; i++) {
    db2free(cols[i]->colName);
    db2free(cols[i]->pgname);
    db2free(cols[i]->val);
    db2free(cols[i]);
  }
  db2free(cols);
  db2free(fdw_state->db2Table);
  db2free(fdw_state->order_clause);
  db2free(fdw_state->where_clause);
  db2Debug1("< db2CleanupFdwState");
}
