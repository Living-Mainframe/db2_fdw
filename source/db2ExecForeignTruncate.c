#include <postgres.h>
#include <utils/rel.h>
#include <nodes/pathnodes.h>
#include <access/xact.h>

#include "db2_fdw.h"
#include "DB2FdwState.h"

/** external prototypes */
extern DB2FdwState* db2GetFdwState       (Oid foreigntableid, double* sample_percent);
extern DB2Session*  db2GetSession        (const char* connectstring, char* user, char* password, char* jwt_token, const char* nls_lang, int curlevel);
extern void         db2PrepareQuery      (DB2Session* session, const char* query, DB2Table* db2Table, unsigned int prefetch);
extern void         db2Debug1            (const char* message, ...);
extern void         db2Debug2            (const char* message, ...);
extern int          db2ExecuteTruncate   (DB2Session* session);
extern void         db2CloseStatement    (DB2Session* session);
extern void         db2Free              (void* p);

/** local prototypes */
DB2FdwState* db2BuildTruncateFdwState(Relation rel, bool restart_seqs);
void         db2ExecForeignTruncate  (List *rels, DropBehavior behavior, bool restart_seqs);

/** ExecForeignTruncate
 *
 * Called once per foreign server. All relations in "rels" must belong
 * to that server.
 */
void db2ExecForeignTruncate(List *rels, DropBehavior behavior, bool restart_seqs) {
  Relation     rel;
  DB2FdwState* fdw_state = NULL;
  ListCell*    lc;

  db2Debug1("> db2ExecForeignTruncate");
  if (rels != NIL) {
    /** Optionally, you could inspect "behavior" (DROP_CASCADE / DROP_RESTRICT)
     * and try to be clever. In practice, Db2 won't cascade TRUNCATE through
     * RI anyway, so we just ignore it and let Db2 raise an error if there
     * are incompatible constraints.
     */
    foreach(lc, rels) {
      /** obtain a fdw_state with a DB session per table */
      rel       = (Relation) lfirst(lc);
      fdw_state = db2BuildTruncateFdwState(rel, restart_seqs);

      /* connect to DB2 database */
      fdw_state->session = db2GetSession(fdw_state->dbserver, fdw_state->user, fdw_state->password, fdw_state->jwt_token, fdw_state->nls_lang, GetCurrentTransactionNestLevel());
      db2PrepareQuery(fdw_state->session, fdw_state->query, fdw_state->db2Table,0);

      /** obtain a fdw_state with a DB session per table */
      db2ExecuteTruncate(fdw_state->session);

      db2CloseStatement (fdw_state->session);
      db2Free(fdw_state->session);
      fdw_state->session = NULL;
    }
  }
  db2Debug1("< db2ExecForeignTruncate");
}

/** db2BuildTruncateFdwState
 * 
 */
DB2FdwState* db2BuildTruncateFdwState(Relation rel, bool restart_seqs) {
  DB2FdwState*   fdwState;
  StringInfoData sql;
  char*          identity_clause;
  char*          storage_clause  = "DROP STORAGE";       /* or REUSE STORAGE */
  char*          trigger_clause  = "IGNORE DELETE TRIGGERS";
  db2Debug1("> db2BuildTruncateFdwState");

  /** Map Postgres' RESTART/CONTINUE IDENTITY to Db2's TRUNCATE options. */
  if (restart_seqs)
      identity_clause = "RESTART IDENTITY";
  else
      identity_clause = "CONTINUE IDENTITY";

      /* Same logic as CMD_INSERT branch of db2PlanForeignModify: */
  fdwState = db2GetFdwState(RelationGetRelid(rel), NULL);
  initStringInfo(&sql);

  /** Build the TRUNCATE TABLE statement.
   *
   * Example:
   *   TRUNCATE TABLE "SCHEMA"."TAB"
   *     DROP STORAGE
   *     IGNORE DELETE TRIGGERS
   *     RESTART IDENTITY
   *     IMMEDIATE
   *
   * You may want to quote identifiers exactly as Db2 expects them.
   */
  appendStringInfo(&sql, "TRUNCATE TABLE %s %s %s %s IMMEDIATE", fdwState->db2Table->name, storage_clause, trigger_clause, identity_clause);
  fdwState->query = sql.data;
  db2Debug2("  fdwState->query: '%s'",sql.data);
  db2Debug1("< db2BuildTruncateFdwState - returns fdwState: %x",fdwState);
  return fdwState;
}
