#include <postgres.h>
#include <nodes/makefuncs.h>

/** external variables */

/** external prototypes */
extern void         db2EndForeignModifyCommon(EState *estate, ResultRelInfo *rinfo);
extern void         db2Debug1                (const char* message, ...);

/** local prototypes */
void                db2EndForeignModify      (EState* estate, ResultRelInfo* rinfo);

/** db2EndForeignModify
 *   Close the currently active DB2 statement.
 */
void db2EndForeignModify (EState* estate, ResultRelInfo* rinfo) {
  db2Debug1("> db2EndForeignModify");
  db2EndForeignModifyCommon(estate, rinfo);
  db2Debug1("< db2EndForeignModify");
}

