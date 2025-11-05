#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */
extern DB2EnvEntry*  rootenvEntry;          /* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void*         db2Alloc             (size_t size);
extern void          db2Debug1            (const char* message, ...);
extern void          db2Debug2            (const char* message, ...);
extern DB2ConnEntry* db2AllocConnHdl      (DB2EnvEntry* envp,const char* srvname, char* user, char* password, const char* nls_lang);
extern DB2EnvEntry*  db2AllocEnvHdl       (const char* nls_lang);
extern DB2EnvEntry*  findenvEntry         (DB2EnvEntry* start, const char* nlslang);
extern void          db2SetSavepoint      (DB2Session* session, int nest_level);

/** local prototypes */
DB2Session*          db2GetSession        (const char* srvname, char* user, char* password, const char* nls_lang, int curlevel);

/** db2GetSession
 *   Look up an DB2 connection in the cache, create a new one if there is none.
 *   The result is a palloc'ed data structure containing the connection.
 *   "curlevel" is the current PostgreSQL transaction level.
 */
DB2Session* db2GetSession (const char* srvname, char* user, char* password, const char* nls_lang, int curlevel) {
  DB2Session*   session = NULL;
  DB2EnvEntry*  envp    = NULL;
  DB2ConnEntry* connp   = NULL;

  db2Debug1("> db2GetSession");
  /* it's easier to deal with empty strings */
  if (!srvname)  srvname  = "";
  if (!user)     user     = "";
  if (!password) password = "";
  if (!nls_lang) nls_lang = "";

  /* search environment and server handle in cache */
  envp = findenvEntry (rootenvEntry, nls_lang);
  if (envp != NULL) {
    db2Debug2("  db2_fdw::db2GetSession: envp: %x, envp->henv: %x",envp,envp->henv);
    connp = db2AllocConnHdl(envp, srvname, user, password, nls_lang);
  }
  if (envp == NULL) {
    envp = db2AllocEnvHdl(nls_lang);
  }
  if (connp == NULL){
    connp = db2AllocConnHdl(envp, srvname, user, password, NULL);
  }
  if (connp->xact_level <= 0) {
    db2Debug2("  db2_fdw::db2GetSession: begin serializable remote transaction");
    connp->xact_level = 1;
  }

  /* palloc a data structure pointing to the cached entries */
  session        = db2Alloc (sizeof (DB2Session));
  session->envp  = envp;
  session->connp = connp;
  session->stmtp = NULL;

  /* set savepoints up to the current level */
  db2SetSavepoint (session, curlevel);

  db2Debug1("< db2GetSession");
  return session;
}
