#include <postgres.h>
#include <foreign/foreign.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include "db2_fdw.h"

/** external prototypes */
extern bool            optionIsTrue              (const char* value);
extern void            db2Debug1                 (const char* message, ...);
extern void            db2Debug2                 (const char* message, ...);

/** local prototypes */
int db2IsForeignRelUpdatable(Relation rel);

/** db2IsForeignRelUpdatable
 *   Returns 0 if "readonly" is set, a value indicating that all DML is allowed.
 */
int db2IsForeignRelUpdatable(Relation rel) {
  ListCell* cell;
  int       result = 0;
  db2Debug1("> db2IsForeignRelUpdatable");
  /* loop foreign table options */
  foreach (cell, GetForeignTable (RelationGetRelid (rel))->options) {
    DefElem *def = (DefElem *) lfirst (cell);
    char *value = STRVAL(def->arg);
    if (strcmp (def->defname, OPT_READONLY) == 0 && optionIsTrue (value))
      return 0;
  }
  result = (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
  db2Debug1("< db2IsForeignRelUpdatable - returns: %d", result);
  return result;
}

