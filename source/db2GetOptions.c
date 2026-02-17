#include <postgres.h>
#include <foreign/foreign.h>
#include <miscadmin.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);
extern void         db2Debug4                 (const char* message, ...);

/** local prototypes */
void db2GetOptions(Oid foreigntableid, List** options);
bool optionIsTrue (const char* value);

/** db2GetOptions
 *   Fetch the options for an db2_fdw foreign table.
 *   Returns a union of the options of the foreign data wrapper,
 *   the foreign server, the user mapping and the foreign table,
 *   in that order. Column options are ignored.
 */
void db2GetOptions (Oid foreigntableid, List** options) {
  ForeignDataWrapper* wrapper = NULL;
  ForeignServer*      server  = NULL;
  UserMapping*        mapping = NULL;
  ForeignTable*       table   = NULL;

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

/* optionIsTrue
 * Returns true if the string is "true", "on" or "yes".
 */
bool optionIsTrue (const char *value) {
  bool result = false;
  db2Debug4("> optionIsTrue(value: '%s')",value);
  result = (pg_strcasecmp (value, "on") == 0 || pg_strcasecmp (value, "yes") == 0 || pg_strcasecmp (value, "true") == 0);
  db2Debug4("< optionIsTrue - returns: '%s'",((result) ? "true" : "false"));
  return result;
}
