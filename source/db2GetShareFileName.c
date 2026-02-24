#include <postgres.h>
#include <miscadmin.h>
#include <optimizer/optimizer.h>
#include <access/heapam.h>
#include <string.h>
#include "db2_fdw.h"

/** external prototypes */
extern void         db2Entry                  (int level, const char* message, ...);
extern void         db2Exit                   (int level, const char* message, ...);
extern void*        db2alloc                  (const char* type, size_t size);

/** local prototypes */
char* db2GetShareFileName(const char *relativename);

/* db2GetShareFileName
 * Returns the allocated absolute path of a file in the "share" directory.
 */
char* db2GetShareFileName (const char *relativename) {
  char share_path[MAXPGPATH], *result;
  db2Entry(1,"> db2GetShareFileName.c::db2GetShareFileName");
  get_share_path(my_exec_path, share_path);
  result = db2alloc("sharedFileName", MAXPGPATH);
  snprintf(result, MAXPGPATH, "%s/%s", share_path, relativename);
  db2Exit(1,"< db2GetShareFileName.c::db2GetShareFileName : '%s'",result);
  return result;
}
