#include <postgres.h>
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
#include <string.h>
#include "db2_fdw.h"

/** external prototypes */
extern void         db2Debug1                 (const char* message, ...);

/** local prototypes */
char* db2GetShareFileName(const char *relativename);

/** db2GetShareFileName
 *   Returns the (palloc'ed) absolute path of a file in the "share" directory.
 */
char* db2GetShareFileName (const char *relativename) {
  char share_path[MAXPGPATH], *result;
  db2Debug1("> db2GetShareFileName");
  get_share_path (my_exec_path, share_path);
  result = palloc (MAXPGPATH);
  snprintf (result, MAXPGPATH, "%s/%s", share_path, relativename);
  db2Debug1("< db2GetShareFileName - returns: '%s'",result);
  return result;
}
