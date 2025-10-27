#include <sqlcli1.h>
#include <postgres_ext.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */
extern DB2EnvEntry* rootenvEntry;          /* contains DB2 error messages, set by db2CheckErr()             */

/** external prototypes */
extern void      db2Debug1            (const char* message, ...);

/** local prototypes */
void             db2Cancel            (void);

/** db2Cancel
 *   Cancel all running DB2 queries.
 */
void db2Cancel (void) {
  DB2EnvEntry*  envp  ;
  DB2ConnEntry* connp ;
  HdlEntry*     entryp;

  db2Debug1("> db2Cancel");
  /* send a cancel request for all servers ignoring errors */
  for (envp = rootenvEntry; envp != NULL; envp = envp->right) {
    for (connp = envp->connlist; connp != NULL; connp = connp->right) {
      for (entryp = connp->handlelist; entryp != NULL; entryp = entryp->next){
        if (entryp->type == SQL_HANDLE_STMT) {
          SQLCancel(entryp->hsql);
        }
      }
    }
  }
  db2Debug1("< db2Cancel");
}
