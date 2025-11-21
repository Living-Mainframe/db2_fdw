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
void* db2alloc0 (const char* type, size_t size);
void* db2alloc  (const char* type, size_t size);
void* db2realloc(void* p, size_t size);
void  db2free   (void* p);
char* db2strdup (const char* source);

/** db2alloc0
 *   Expose palloc0() to DB2 functions.
 */
void* db2alloc0 (const char* type, size_t size) {
  void* memory = palloc0(size);
  db2Debug1("  ++ %x: %d bytes - %s", memory, size, type);
  return memory;
}

/** db2alloc
 *   Expose palloc() to DB2 functions.
 */
void* db2alloc (const char* type, size_t size) {
  void* memory = palloc(size);
  db2Debug1("  ++ %x: %d bytes - %s", memory, size, type);
  return memory;
}

/** db2realloc
 *   Expose repalloc() to DB2 functions.
 */
void* db2realloc (void* p, size_t size) {
  void* memory = repalloc(p, size);
  db2Debug1("  ++ %x: %d bytes", memory, size);
  return memory;
}
/** db2free
 *   Expose pfree() to DB2 functions.
 */
void db2free (void* p) {
  if (p != NULL) {
    db2Debug1("  -- %x", p);
    pfree (p);
  }
}

char* db2strdup(const char* source) {
  char* target = NULL;
  if (source != NULL && source[0] != '\0') {
    target = pstrdup(source);
  }
  db2Debug1("  ++ %x: dup'ed string from %x content '%s'",target, source, source);
  return target;
}