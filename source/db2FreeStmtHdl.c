#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** external variables */

/** external prototypes */
extern void      db2Debug1            (const char* message, ...);
extern void      db2Debug2            (const char* message, ...);
extern void      db2Error             (db2error sqlstate, const char* message);
extern SQLRETURN db2CheckErr          (SQLRETURN status, SQLHANDLE handle, SQLSMALLINT handleType, int line, char* file);

/** local prototypes */
void             db2FreeStmtHdl       (HdlEntry* handlep, DB2ConnEntry* connp);
HdlEntry*        findhdlEntry         (HdlEntry* start, SQLHANDLE hsql);

/** db2FreeStmtHdl
 *   Free an DB2 statement handle, remove it from the cached list.
 */
void db2FreeStmtHdl (HdlEntry* handlep, DB2ConnEntry* connp) {
  HdlEntry* entryp     = NULL;
  HdlEntry* preventryp = NULL;
  SQLRETURN rc         = 0;

  db2Debug1("> db2FreeStmtHdl");
  db2Debug2("  hnadlep: %x", handlep);
  db2Debug2("  connp  : %x", connp);

  entryp = findhdlEntry(connp->handlelist, handlep->hsql);
  if (entryp == NULL) {
    db2Error (FDW_ERROR, "internal error freeing handle: not found in cache");
  }

  db2Debug2("  hsql: %d", entryp->hsql);
  db2Debug2("  type: %d", entryp->type);
  db2Debug2("  next: %x", entryp->next);
  /* free the handle */
  rc = SQLFreeHandle(entryp->type, entryp->hsql);
  rc = db2CheckErr(rc, entryp->hsql, entryp->type, __LINE__, __FILE__ );

  /* remove it */
  db2Debug2("  preveventryp       : %x", preventryp);
  if (preventryp == NULL) {
    db2Debug2("  connp_handlelist       : '%x'", connp->handlelist);
    if (entryp->next == NULL){
      db2Debug2("  set connp->handlelist to NULL'");
      connp->handlelist = NULL;
    } else {
      connp->handlelist = entryp->next;
    }
  } else {
    db2Debug2("  preveventryp->next : '%x'", preventryp->next);
    preventryp->next = entryp->next;
    db2Debug2("  preveventryp->next : '%x'", preventryp->next);
  }
  db2Debug2("  free(entryp): '%x'", entryp);
  free (entryp);
  db2Debug1("< db2FreeStmtHdl");
}

/** findhdlEntry
 * 
 */
HdlEntry* findhdlEntry (HdlEntry* start, SQLHANDLE hsql) {
  HdlEntry* step = NULL;
  db2Debug1("> findhdlEntry");
  for (step = start; step != NULL; step = step->next){
    if (step->hsql == hsql) {
      break;
    }
  }
  db2Debug1("< findhdlEntry - returns: %x", step);
  return step;
}
