#include <postgres.h>
//#include "db2_pg.h"
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

/*+ external prototypes */
extern void         db2Debug1                 (const char* message, ...);

/** local prototypes */
void* db2Alloc  (size_t size);
void* db2Realloc(void* p, size_t size);
void  db2Free   (void* p);

/** db2Alloc
 *   Expose palloc() to DB2 functions.
 */
void* db2Alloc (size_t size) {
  void* memory = palloc(size);
  db2Debug1("  allocate %d bytes  : %x", size, memory);
  return memory;
}

/** db2Realloc
 *   Expose repalloc() to DB2 functions.
 */
void* db2Realloc (void *p, size_t size) {
  void* memory = repalloc(p, size);
  db2Debug1("  reallocated %d bytes: %x from %x", size, memory, p);
  return memory;
}
/** db2Free
 *   Expose pfree() to DB2 functions.
 */
void db2Free (void *p) {
  db2Debug1("  freed mem  : %x",p);
  pfree (p);
}
