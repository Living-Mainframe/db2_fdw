#include <postgres.h>
#include <nodes/makefuncs.h>

/** external variables */

/** external prototypes */
extern void         db2Debug1                (const char* message, ...);
extern void         db2EndForeignModifyCommon(EState *estate, ResultRelInfo *rinfo);

/** local prototypes */
void            db2EndForeignInsert      (EState* estate, ResultRelInfo* rinfo);

/** db2EndForeignInsert
 *   Close the currently active DB2 statement.
 */
void db2EndForeignInsert (EState* estate, ResultRelInfo* rinfo) {
  db2Debug1("> db2EndForeignInsert");
  db2EndForeignModifyCommon(estate, rinfo);
  db2Debug1("< db2EndForeignInsert");
}

