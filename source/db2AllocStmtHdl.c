#include <string.h>
#include <unistd.h>
#include <pthread.h>
#if defined _WIN32 || defined _WIN64
/* for getpid */
#include <process.h>
/* Windows doesn't have snprintf */
#define snprintf _snprintf
#endif
#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** external variables */
extern DB2EnvEntry* rootenvEntry;          /* Linked list of handles for cached DB2 connections.            */

/** external prototypes */
extern int       isLogLevel           (int level);
extern void      db2Entry             (int level, const char* message, ...);
extern void      db2Exit              (int level, const char* message, ...);
extern void      db2Debug             (int level, const char* message, ...);
extern void      db2Error             (db2error sqlstate, const char* message);
extern void      db2Error_d           (db2error sqlstate, const char* message, const char* detail, ...);

/** inetrnal prototypes */
HdlEntry*  db2AllocStmtHdl     (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg);

/* db2AllocStmtHdl
 * Allocate an DB2 statement handle, keep it in the cached list.
 */
HdlEntry* db2AllocStmtHdl (SQLSMALLINT type, DB2ConnEntry* connp, db2error error, const char* errmsg) {
  HdlEntry*     entry   = NULL;
  SQLRETURN     rc      = 0;

  db2Entry(1,"> db2AllocStmtHdl.c::db2AllocStmtHdl");
  if (isLogLevel(5)) {
    DB2EnvEntry*  envstep = NULL;
    DB2ConnEntry* constep = NULL;
    HdlEntry*     hdlstep = NULL;

    db2Debug(5,"struct before calling pthread_create getpid: %d getpthread_self: %d", getpid(), (int)pthread_self());
    for (envstep = rootenvEntry; envstep != NULL; envstep = envstep->right){
      db2Debug(5,"EnvEntry               : %x",envstep);
      db2Debug(5,"  nls_lang               : %s",envstep->nls_lang);
      db2Debug(5,"  step->henv             : %x",envstep->henv);
      db2Debug(5,"  step->*left            : %x",envstep->left);
      db2Debug(5,"  step->*right           : %x",envstep->right);
      db2Debug(5,"  step->*connlist        : %x",envstep->connlist);
      for (constep = envstep->connlist; constep != NULL; constep = constep->right){
        db2Debug(5,"    ConnEntr             : %x",constep);
        db2Debug(5,"      dbAlias              : %s",constep->srvname);
        db2Debug(5,"      user                 : %s",constep->uid);
        db2Debug(5,"      password             : %s",constep->pwd);
        db2Debug(5,"      xact_level           : %d",constep->xact_level);
        db2Debug(5,"      conattr              : %d",constep->conAttr);
        db2Debug(5,"      *handlelist          : %x",constep->handlelist);
        db2Debug(5,"      DB2ConnEntry *left   : %x",constep->left);
        db2Debug(5,"      Db2ConnEntry *right  : %x",constep->right);
        for (hdlstep = constep->handlelist; hdlstep != NULL; hdlstep = hdlstep->next){
          db2Debug(5,"        HandleEntry        : %x",hdlstep);
          db2Debug(5,"          hsql               : %d",hdlstep->hsql);
          db2Debug(5,"          type               : %d",hdlstep->type);
        }
      }
    }
  }
  /* create entry for linked list */
  if ((entry = malloc (sizeof (HdlEntry))) == NULL) {
    db2Error_d (FDW_OUT_OF_MEMORY, "error allocating handle:"," failed to allocate %d bytes of memory", sizeof (HdlEntry));
  }
  db2Debug(2,"HdlEntry allocated: %x",entry);
  rc = SQLAllocHandle(type, connp->hdbc, &(entry->hsql));
  if (rc != SQL_SUCCESS) {
    db2Debug(3,"SQLAllocHandle not SQL_SUCCESS: %d",rc);
    db2Debug(2,"HdlEntry freeed: %x",entry);
    free (entry);
    entry = NULL;
    db2Error (error, errmsg);
  } else {
    /* add handle to linked list */
    db2Debug(3,"entry->hsql: %d",entry->hsql);
    entry->type         = type;
    db2Debug(3,"entry->type: %d",entry->type);
    entry->next         = connp->handlelist;
    db2Debug(3,"adding connp->handlelist: %x to entry->next: %x",connp->handlelist, entry->next);
    connp->handlelist   = entry;
    db2Debug(3,"set entry %x to start connp->handlelist: %x",entry,connp->handlelist);
  }
  db2Exit(1,"< db2AllocStmtHdl.c::db2AllocStmtHdl : %x",entry);
  return entry;
}
