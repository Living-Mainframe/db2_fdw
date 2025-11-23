#include <postgres.h>
#include <foreign/foreign.h>
#include <miscadmin.h>
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

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);

/** local prototypes */
void db2GetOptions(Oid foreigntableid, List** options);

/** db2GetOptions
 *   Fetch the options for an db2_fdw foreign table.
 *   Returns a union of the options of the foreign data wrapper,
 *   the foreign server, the user mapping and the foreign table,
 *   in that order. Column options are ignored.
 */
void db2GetOptions (Oid foreigntableid, List** options) {
  ForeignTable*       table   = NULL;
  ForeignServer*      server  = NULL;
  UserMapping*        mapping = NULL;
  ForeignDataWrapper* wrapper = NULL;

  db2Debug1("> db2GetOptions");
  /** Gather all data for the foreign table. */
  table = GetForeignTable(foreigntableid);
  if (table != NULL) {
    server  = GetForeignServer(table->serverid);
    mapping = GetUserMapping(GetUserId(), table->serverid);
    if (server != NULL) {
      wrapper = GetForeignDataWrapper(server->fdwid);
    } else {
        db2Debug1("  unable to GetForeignServer: %d", table->serverid);
    }
    /* later options override earlier ones */
    *options = NIL;
    if (wrapper != NULL)
      *options = list_concat(*options, wrapper->options);
    else
      db2Debug1("  unable to get wrapper options");
    if (server != NULL)
      *options = list_concat(*options, server->options);
    else
      db2Debug1("  unable to get server options");
    if (mapping != NULL)
      *options = list_concat(*options, mapping->options);
    else
      db2Debug1("  unable to get mapping options");
    *options = list_concat(*options, table->options);
  } else {
    db2Debug1("  unable to GetForeignTable: %d",foreigntableid);
  }
  db2Debug1("< db2GetOptions");
}